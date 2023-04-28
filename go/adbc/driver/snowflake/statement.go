// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package snowflake

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/exp/constraints"
)

const (
	OptionStatementQueueSize = "adbc.rpc.result_queue_size"
)

type statement struct {
	cnxn      *cnxn
	alloc     memory.Allocator
	queueSize int

	query       string
	targetTable string
	append      bool

	bound      arrow.Record
	streamBind array.RecordReader
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (st *statement) Close() error {
	if st.cnxn == nil {
		return adbc.Error{
			Msg:  "statement already closed",
			Code: adbc.StatusInvalidState}
	}

	if st.bound != nil {
		st.bound.Release()
		st.bound = nil
	} else if st.streamBind != nil {
		st.streamBind.Release()
		st.streamBind = nil
	}
	st.cnxn = nil
	return nil
}

// SetOption sets a string option on this statement
func (st *statement) SetOption(key string, val string) error {
	switch key {
	case adbc.OptionKeyIngestTargetTable:
		st.query = ""
		st.targetTable = val
	case adbc.OptionKeyIngestMode:
		switch val {
		case adbc.OptionValueIngestModeAppend:
			st.append = true
		case adbc.OptionValueIngestModeCreate:
			st.append = false
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid statement option %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	default:
		return adbc.Error{
			Msg:  fmt.Sprintf("invalid statement option %s=%s", key, val),
			Code: adbc.StatusInvalidArgument,
		}
	}
	return nil
}

// SetSqlQuery sets the query string to be executed.
//
// The query can then be executed with any of the Execute methods.
// For queries expected to be executed repeatedly, Prepare should be
// called before execution.
func (st *statement) SetSqlQuery(query string) error {
	st.query = query
	st.targetTable = ""
	return nil
}

func toSnowflakeType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.EXTENSION:
		return toSnowflakeType(dt.(arrow.ExtensionType).StorageType())
	case arrow.DICTIONARY:
		return toSnowflakeType(dt.(*arrow.DictionaryType).ValueType)
	case arrow.RUN_END_ENCODED:
		return toSnowflakeType(dt.(*arrow.RunEndEncodedType).Encoded())
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return "integer"
	case arrow.FLOAT32, arrow.FLOAT16, arrow.FLOAT64:
		return "double"
	case arrow.DECIMAL, arrow.DECIMAL256:
		dec := dt.(arrow.DecimalType)
		return fmt.Sprintf("NUMERIC(%d,%d)", dec.GetPrecision(), dec.GetScale())
	case arrow.STRING, arrow.LARGE_STRING:
		return "text"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "binary"
	case arrow.FIXED_SIZE_BINARY:
		fsb := dt.(*arrow.FixedSizeBinaryType)
		return fmt.Sprintf("binary(%d)", fsb.ByteWidth)
	case arrow.BOOL:
		return "boolean"
	case arrow.TIME32, arrow.TIME64:
		t := dt.(arrow.TemporalWithUnit)
		prec := int(t.TimeUnit()) * 3
		return fmt.Sprintf("time(%d)", prec)
	case arrow.DATE32, arrow.DATE64:
		return "date"
	case arrow.TIMESTAMP:
		ts := dt.(*arrow.TimestampType)
		prec := int(ts.Unit) * 3
		if ts.TimeZone == "" {
			return fmt.Sprintf("timestamp_tz(%d)", prec)
		}
		return fmt.Sprintf("timestamp_ltz(%d)", prec)
	case arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return "variant"
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return "array"
	case arrow.STRUCT, arrow.MAP:
		return "object"
	}

	return ""
}

func (st *statement) initIngest(ctx context.Context) (string, error) {
	var (
		createBldr, insertBldr strings.Builder
	)

	createBldr.WriteString("CREATE TABLE ")
	createBldr.WriteString(st.targetTable)
	createBldr.WriteString(" (")

	insertBldr.WriteString("INSERT INTO ")
	insertBldr.WriteString(st.targetTable)
	insertBldr.WriteString(" VALUES (")

	var schema *arrow.Schema
	if st.bound != nil {
		schema = st.bound.Schema()
	} else {
		schema = st.streamBind.Schema()
	}

	for i, f := range schema.Fields() {
		if i != 0 {
			insertBldr.WriteString(", ")
			createBldr.WriteString(", ")
		}

		createBldr.WriteString(strconv.Quote(f.Name))
		createBldr.WriteString(" ")
		ty := toSnowflakeType(f.Type)
		if ty == "" {
			return "", adbc.Error{
				Msg:  fmt.Sprintf("unimplemented type conversion for field %s, arrow type: %s", f.Name, f.Type),
				Code: adbc.StatusNotImplemented,
			}
		}

		createBldr.WriteString(ty)
		if !f.Nullable {
			createBldr.WriteString(" NOT NULL")
		}

		insertBldr.WriteString("?")
	}

	createBldr.WriteString(")")
	insertBldr.WriteString(")")

	if !st.append {
		// create the table!
		createQuery := createBldr.String()
		_, err := st.cnxn.cn.ExecContext(ctx, createQuery, nil)
		if err != nil {
			return "", errToAdbcErr(adbc.StatusInternal, err)
		}
	}

	return insertBldr.String(), nil
}

type nativeArrowArr[T string | []byte] interface {
	arrow.Array
	Value(int) T
}

func convToArr[T string | []byte](arr nativeArrowArr[T]) interface{} {
	if arr.Len() == 1 {
		if arr.IsNull(0) {
			return nil
		}

		return arr.Value(0)
	}

	v := make([]interface{}, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			continue
		}
		v[i] = arr.Value(i)
	}
	return gosnowflake.Array(&v)
}

func convMarshal(arr arrow.Array) interface{} {
	if arr.Len() == 0 {
		if arr.IsNull(0) {
			return nil
		}
		return arr.ValueStr(0)
	}

	v := make([]interface{}, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		if arr.IsNull(i) {
			continue
		}
		v[i] = arr.ValueStr(i)
	}
	return gosnowflake.Array(&v)
}

// snowflake driver bindings only support specific types
// int/int32/int64/float64/float32/bool/string/byte/time
// so we have to cast anything else appropriately
func convToSlice[T, O constraints.Integer | constraints.Float](arr arrow.Array, vals []T) interface{} {
	if arr.Len() == 1 {
		if arr.IsNull(0) {
			return nil
		}

		return vals[0]
	}

	out := make([]interface{}, arr.Len())
	for i, v := range vals {
		if arr.IsNull(i) {
			continue
		}
		out[i] = O(v)
	}
	return gosnowflake.Array(&out)
}

func getQueryArg(arr arrow.Array) interface{} {
	switch arr := arr.(type) {
	case *array.Int8:
		v := arr.Int8Values()
		return convToSlice[int8, int32](arr, v)
	case *array.Uint8:
		v := arr.Uint8Values()
		return convToSlice[uint8, int32](arr, v)
	case *array.Int16:
		v := arr.Int16Values()
		return convToSlice[int16, int32](arr, v)
	case *array.Uint16:
		v := arr.Uint16Values()
		return convToSlice[uint16, int32](arr, v)
	case *array.Int32:
		v := arr.Int32Values()
		return convToSlice[int32, int32](arr, v)
	case *array.Uint32:
		v := arr.Uint32Values()
		return convToSlice[uint32, int64](arr, v)
	case *array.Int64:
		v := arr.Int64Values()
		return convToSlice[int64, int64](arr, v)
	case *array.Uint64:
		v := arr.Uint64Values()
		return convToSlice[uint64, int64](arr, v)
	case *array.Float32:
		v := arr.Float32Values()
		return convToSlice[float32, float64](arr, v)
	case *array.Float64:
		v := arr.Float64Values()
		return convToSlice[float64, float64](arr, v)
	case *array.LargeBinary:
		return convToArr[[]byte](arr)
	case *array.Binary:
		return convToArr[[]byte](arr)
	case *array.LargeString:
		return convToArr[string](arr)
	case *array.String:
		return convToArr[string](arr)
	default:
		// default convert to array of strings and pass to snowflake driver
		// not the most efficient, but snowflake doesn't really give a better
		// route currently short of writing everything out to a Parquet file
		// and then uploading that (which might be preferable)
		return convMarshal(arr)
	}
}

func (st *statement) executeIngest(ctx context.Context) (int64, error) {
	if st.streamBind == nil && st.bound == nil {
		return -1, adbc.Error{
			Msg:  "must call Bind before bulk ingestion",
			Code: adbc.StatusInvalidState,
		}
	}

	insertQuery, err := st.initIngest(ctx)
	if err != nil {
		return -1, err
	}

	// if the ingestion is large enough it might make more sense to
	// write this out to a temporary file / stage / etc. and use
	// the snowflake bulk loader that way.
	//
	// on the other hand, according to the documentation,
	// https://pkg.go.dev/github.com/snowflakedb/gosnowflake#hdr-Batch_Inserts_and_Binding_Parameters
	// snowflake's internal driver work should already be doing this.

	var n int64
	exec := func(rec arrow.Record, args []driver.NamedValue) error {
		for i, c := range rec.Columns() {
			args[i].Ordinal = i
			args[i].Value = getQueryArg(c)
		}

		r, err := st.cnxn.cn.ExecContext(ctx, insertQuery, args)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}

		rows, err := r.RowsAffected()
		if err == nil {
			n += rows
		}
		return nil
	}

	if st.bound != nil {
		defer func() {
			st.bound.Release()
			st.bound = nil
		}()
		args := make([]driver.NamedValue, len(st.bound.Schema().Fields()))
		return n, exec(st.bound, args)
	}

	defer func() {
		st.streamBind.Release()
		st.streamBind = nil
	}()
	args := make([]driver.NamedValue, len(st.streamBind.Schema().Fields()))
	for st.streamBind.Next() {
		rec := st.streamBind.Record()
		if err := exec(rec, args); err != nil {
			return n, err
		}
	}

	return n, nil
}

// ExecuteQuery executes the current query or prepared statement
// and returnes a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (st *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if st.targetTable != "" {
		n, err := st.executeIngest(ctx)
		return nil, n, err
	}

	if st.query == "" {
		return nil, -1, adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
	}

	// for a bound stream reader we'd need to implement something to
	// concatenate RecordReaders which doesn't exist yet. let's put
	// that off for now.
	if st.streamBind != nil || st.bound != nil {
		return nil, -1, adbc.Error{
			Msg:  "executing non-bulk ingest with bound params not yet implemented",
			Code: adbc.StatusNotImplemented,
		}
	}

	loader, err := st.cnxn.cn.QueryArrowStream(ctx, st.query)
	if err != nil {
		return nil, -1, errToAdbcErr(adbc.StatusInternal, err)
	}

	rdr, err := newRecordReader(ctx, st.alloc, loader, st.queueSize)
	nrec := loader.TotalRows()
	return rdr, nrec, err
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (st *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	if st.targetTable != "" {
		return st.executeIngest(ctx)
	}

	if st.query == "" {
		return -1, adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
	}

	r, err := st.cnxn.cn.ExecContext(ctx, st.query, nil)
	if err != nil {
		return -1, errToAdbcErr(adbc.StatusIO, err)
	}

	n, err := r.RowsAffected()
	if err != nil {
		n = -1
	}

	return n, nil
}

// Prepare turns this statement into a prepared statement to be executed
// multiple times. This invalidates any prior result sets.
func (st *statement) Prepare(_ context.Context) error {
	if st.query == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "cannot prepare statement with no query",
		}
	}
	// snowflake doesn't provide a "Prepare" api, this is a no-op
	return nil
}

// SetSubstraitPlan allows setting a serialized Substrait execution
// plan into the query or for querying Substrait-related metadata.
//
// Drivers are not required to support both SQL and Substrait semantics.
// If they do, it may be via converting between representations internally.
//
// Like SetSqlQuery, after this is called the query can be executed
// using any of the Execute methods. If the query is expected to be
// executed repeatedly, Prepare should be called first on the statement.
func (st *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Msg:  "Snowflake does not support Substrait plans",
		Code: adbc.StatusNotImplemented,
	}
}

// Bind uses an arrow record batch to bind parameters to the query.
//
// This can be used for bulk inserts or for prepared statements.
// The driver will call release on the passed in Record when it is done,
// but it may not do this until the statement is closed or another
// record is bound.
func (st *statement) Bind(_ context.Context, values arrow.Record) error {
	if st.streamBind != nil {
		st.streamBind.Release()
		st.streamBind = nil
	} else if st.bound != nil {
		st.bound.Release()
		st.bound = nil
	}

	st.bound = values
	if st.bound != nil {
		st.bound.Retain()
	}
	return nil
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
//
// The driver will call Release on the record reader, but may not do this
// until Close is called.
func (st *statement) BindStream(_ context.Context, stream array.RecordReader) error {
	if st.streamBind != nil {
		st.streamBind.Release()
		st.streamBind = nil
	} else if st.bound != nil {
		st.bound.Release()
		st.bound = nil
	}

	st.streamBind = stream
	if st.streamBind != nil {
		st.streamBind.Retain()
	}
	return nil
}

// GetParameterSchema returns an Arrow schema representation of
// the expected parameters to be bound.
//
// This retrieves an Arrow Schema describing the number, names, and
// types of the parameters in a parameterized statement. The fields
// of the schema should be in order of the ordinal position of the
// parameters; named parameters should appear only once.
//
// If the parameter does not have a name, or a name cannot be determined,
// the name of the corresponding field in the schema will be an empty
// string. If the type cannot be determined, the type of the corresponding
// field will be NA (NullType).
//
// This should be called only after calling Prepare.
//
// This should return an error with StatusNotImplemented if the schema
// cannot be determined.
func (st *statement) GetParameterSchema() (*arrow.Schema, error) {
	// snowflake's API does not provide any way to determine the schema
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
	}
}

// ExecutePartitions executes the current statement and gets the results
// as a partitioned result set.
//
// It returns the Schema of the result set, the collection of partition
// descriptors and the number of rows affected, if known. If unknown,
// the number of rows affected will be -1.
//
// If the driver does not support partitioned results, this will return
// an error with a StatusNotImplemented code.
func (st *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	if st.query == "" {
		return nil, adbc.Partitions{}, -1, adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
	}

	// snowflake partitioned results are not currently portable enough to
	// satisfy the requirements of this function. At least not what is
	// returned from the snowflake driver.
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Msg:  "ExecutePartitions not implemented for Snowflake",
		Code: adbc.StatusNotImplemented,
	}
}
