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
	"io"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	OptionStatementQueryTag                = "adbc.snowflake.statement.query_tag"
	OptionStatementQueueSize               = "adbc.rpc.result_queue_size"
	OptionStatementPrefetchConcurrency     = "adbc.snowflake.rpc.prefetch_concurrency"
	OptionStatementIngestWriterConcurrency = "adbc.snowflake.statement.ingest_writer_concurrency"
	OptionStatementIngestUploadConcurrency = "adbc.snowflake.statement.ingest_upload_concurrency"
	OptionStatementIngestCopyConcurrency   = "adbc.snowflake.statement.ingest_copy_concurrency"
	OptionStatementIngestTargetFileSize    = "adbc.snowflake.statement.ingest_target_file_size"
	OptionStatementIngestCompressionCodec  = "adbc.snowflake.statement.ingest_compression_codec" // TODO(GH-1473): Implement option
	OptionStatementIngestCompressionLevel  = "adbc.snowflake.statement.ingest_compression_level" // TODO(GH-1473): Implement option
)

type statement struct {
	driverbase.StatementImplBase
	cnxn                  *connectionImpl
	alloc                 memory.Allocator
	queueSize             int
	prefetchConcurrency   int
	useHighPrecision      bool
	maxTimestampPrecision MaxTimestampPrecision

	query         string
	targetTable   string
	ingestMode    string
	ingestOptions *ingestOptions
	queryTag      string

	bound      arrow.RecordBatch
	streamBind array.RecordReader
}

func (st *statement) Base() *driverbase.StatementImplBase {
	return &st.StatementImplBase
}

// setQueryContext applies the query tag if present.
func (st *statement) setQueryContext(ctx context.Context) context.Context {
	if st.queryTag != "" {
		ctx = gosnowflake.WithQueryTag(ctx, st.queryTag)
	}
	return ctx
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (st *statement) Close() (err error) {
	_, span := internal.StartSpan(context.Background(), "statement.Close", st)
	defer internal.EndSpan(span, err)

	if st.cnxn == nil {
		err = adbc.Error{
			Msg:  "statement already closed",
			Code: adbc.StatusInvalidState}
		return err
	}

	if st.bound != nil {
		st.bound.Release()
		st.bound = nil
	} else if st.streamBind != nil {
		st.streamBind.Release()
		st.streamBind = nil
	}
	st.cnxn = nil
	return err
}

func (st *statement) GetOption(key string) (string, error) {
	switch key {
	case OptionStatementQueryTag:
		return st.queryTag, nil
	default:
		return st.Base().GetOption(key)
	}
}

func (st *statement) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionStatementQueueSize:
		return int64(st.queueSize), nil
	}
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}
func (st *statement) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
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
			fallthrough
		case adbc.OptionValueIngestModeCreate:
			fallthrough
		case adbc.OptionValueIngestModeReplace:
			fallthrough
		case adbc.OptionValueIngestModeCreateAppend:
			st.ingestMode = val
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] invalid statement option %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	case OptionStatementQueueSize:
		sz, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(key, int64(sz))
	case OptionStatementPrefetchConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(key, int64(concurrency))
	case OptionStatementIngestWriterConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(key, int64(concurrency))
	case OptionStatementIngestUploadConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(key, int64(concurrency))
	case OptionStatementIngestCopyConcurrency:
		concurrency, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(key, int64(concurrency))
	case OptionStatementIngestTargetFileSize:
		size, err := strconv.Atoi(val)
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] could not parse '%s' as int for option '%s'", val, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		return st.SetOptionInt(key, int64(size))
	case OptionStatementQueryTag:
		st.queryTag = val
		return nil
	case OptionUseHighPrecision:
		switch val {
		case adbc.OptionValueEnabled:
			st.useHighPrecision = true
		case adbc.OptionValueDisabled:
			st.useHighPrecision = false
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] invalid statement option %s=%s", key, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
	default:
		return st.Base().SetOption(key, val)
	}
	return nil
}

func (st *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionStatementQueueSize:
		if value <= 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Snowflake] Invalid value for statement option '%s': '%d' is not a positive integer", OptionStatementQueueSize, value),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.queueSize = int(value)
		return nil
	case OptionStatementPrefetchConcurrency:
		if value <= 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be > 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.prefetchConcurrency = int(value)
		return nil
	case OptionStatementIngestWriterConcurrency:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		if value == 0 {
			st.ingestOptions.writerConcurrency = defaultWriterConcurrency
			return nil
		}

		st.ingestOptions.writerConcurrency = uint(value)
		return nil
	case OptionStatementIngestUploadConcurrency:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		if value == 0 {
			st.ingestOptions.uploadConcurrency = defaultUploadConcurrency
			return nil
		}

		st.ingestOptions.uploadConcurrency = uint(value)
		return nil
	case OptionStatementIngestCopyConcurrency:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.ingestOptions.copyConcurrency = uint(value)
		return nil
	case OptionStatementIngestTargetFileSize:
		if value < 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("invalid value ('%d') for option '%s', must be >= 0", value, key),
				Code: adbc.StatusInvalidArgument,
			}
		}
		st.ingestOptions.targetFileSize = uint(value)
		return nil
	}
	return adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Snowflake] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
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
	case arrow.STRING, arrow.LARGE_STRING, arrow.STRING_VIEW:
		return "text"
	case arrow.BINARY, arrow.LARGE_BINARY, arrow.BINARY_VIEW:
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
			return fmt.Sprintf("timestamp_ntz(%d)", prec)
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

func (st *statement) initIngest(ctx context.Context) error {
	var (
		createBldr strings.Builder
	)

	createBldr.WriteString("CREATE TABLE ")
	if st.ingestMode == adbc.OptionValueIngestModeCreateAppend {
		createBldr.WriteString(" IF NOT EXISTS ")
	}
	createBldr.WriteString(quoteTblName(st.targetTable))
	createBldr.WriteString(" (")

	var schema *arrow.Schema
	if st.bound != nil {
		schema = st.bound.Schema()
	} else {
		schema = st.streamBind.Schema()
	}

	for i, f := range schema.Fields() {
		if i != 0 {
			createBldr.WriteString(", ")
		}

		createBldr.WriteString(quoteTblName(f.Name))
		createBldr.WriteString(" ")
		ty := toSnowflakeType(f.Type)
		if ty == "" {
			return adbc.Error{
				Msg:  fmt.Sprintf("unimplemented type conversion for field %s, arrow type: %s", f.Name, f.Type),
				Code: adbc.StatusNotImplemented,
			}
		}

		createBldr.WriteString(ty)
		if !f.Nullable {
			createBldr.WriteString(" NOT NULL")
		}
	}

	createBldr.WriteString(")")

	switch st.ingestMode {
	case adbc.OptionValueIngestModeAppend:
		// Do nothing
	case adbc.OptionValueIngestModeReplace:
		replaceQuery := "DROP TABLE IF EXISTS " + quoteTblName(st.targetTable)
		_, err := st.cnxn.cn.ExecContext(ctx, replaceQuery, nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}

		fallthrough
	case adbc.OptionValueIngestModeCreate:
		fallthrough
	case adbc.OptionValueIngestModeCreateAppend:
		fallthrough
	default:
		// create the table!
		createQuery := createBldr.String()
		_, err := st.cnxn.cn.ExecContext(ctx, createQuery, nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}
	}

	return nil
}

func (st *statement) executeIngest(ctx context.Context) (int64, error) {
	if st.streamBind == nil && st.bound == nil {
		return -1, adbc.Error{
			Msg:  "must call Bind before bulk ingestion",
			Code: adbc.StatusInvalidState,
		}
	}

	err := st.initIngest(ctx)
	if err != nil {
		return -1, err
	}

	if st.bound != nil {
		return st.ingestRecord(ctx)
	}

	return st.ingestStream(ctx)
}

// ExecuteQuery executes the current query or prepared statement
// and returns a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (st *statement) ExecuteQuery(ctx context.Context) (reader array.RecordReader, nRows int64, err error) {
	nRows = -1

	var span trace.Span
	ctx, span = internal.StartSpan(ctx, "statement.ExecuteQuery", st)
	defer func() {
		span.SetAttributes(semconv.DBResponseReturnedRowsKey.Int64(nRows))
		internal.EndSpan(span, err)
	}()

	ctx = st.setQueryContext(ctx)

	if st.targetTable != "" {
		nRows, err = st.executeIngest(ctx)
		return
	}

	if st.query == "" {
		err = adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
		return
	}

	// for a bound stream reader we'd need to implement something to
	// concatenate RecordReaders which doesn't exist yet. let's put
	// that off for now.
	if st.streamBind != nil || st.bound != nil {
		bind := snowflakeBindReader{
			doQuery: func(params []driver.NamedValue) (array.RecordReader, error) {
				var loader gosnowflake.ArrowStreamLoader
				loader, err = st.cnxn.cn.QueryArrowStream(ctx, st.query, params...)
				if err != nil {
					err = errToAdbcErr(adbc.StatusInternal, err)
					return nil, err
				}

				reader, err = newRecordReader(ctx, st.alloc, loader, st.queueSize, st.prefetchConcurrency, st.useHighPrecision, st.maxTimestampPrecision)
				return reader, err
			},
			currentBatch: st.bound,
			stream:       st.streamBind,
		}
		st.bound = nil
		st.streamBind = nil

		rdr := concatReader{}
		err = rdr.Init(&bind)
		if err != nil {
			return
		}
		reader = &rdr
		return
	}

	var loader gosnowflake.ArrowStreamLoader
	loader, err = st.cnxn.cn.QueryArrowStream(ctx, st.query)
	if err != nil {
		err = errToAdbcErr(adbc.StatusInternal, err)
		return
	}

	reader, err = newRecordReader(ctx, st.alloc, loader, st.queueSize, st.prefetchConcurrency, st.useHighPrecision, st.maxTimestampPrecision)
	nRows = loader.TotalRows()
	return
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (st *statement) ExecuteUpdate(ctx context.Context) (numRows int64, err error) {
	ctx, span := internal.StartSpan(ctx, "statement.ExecuteUpdate", st)
	defer func() {
		span.SetAttributes(semconv.DBResponseReturnedRowsKey.Int64(numRows))
		internal.EndSpan(span, err)
	}()

	ctx = st.setQueryContext(ctx)

	if st.targetTable != "" {
		numRows, err = st.executeIngest(ctx)
		return numRows, err
	}

	if st.query == "" {
		numRows = -1
		err = adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
		return numRows, err
	}

	if st.streamBind != nil || st.bound != nil {
		numRows = 0
		bind := snowflakeBindReader{
			currentBatch: st.bound,
			stream:       st.streamBind,
		}
		st.bound = nil
		st.streamBind = nil

		defer bind.Release()
		for {
			params, err := bind.NextParams()
			if err == io.EOF {
				break
			} else if err != nil {
				numRows = -1
				return numRows, err
			}
			r, err := st.cnxn.cn.ExecContext(ctx, st.query, params)
			if err != nil {
				err = errToAdbcErr(adbc.StatusInternal, err)
				numRows = -1
				return numRows, err
			}
			n, err := r.RowsAffected()
			if err != nil {
				numRows = -1
			} else if numRows >= 0 {
				numRows += n
			}
		}
		err = nil
		return numRows, err
	}

	r, err := st.cnxn.cn.ExecContext(ctx, st.query, nil)
	if err != nil {
		numRows = -1
		err = errToAdbcErr(adbc.StatusIO, err)
		return numRows, err
	}

	numRows, err = r.RowsAffected()
	if err != nil {
		numRows = -1
		err = nil
	}

	return numRows, err
}

// ExecuteSchema gets the schema of the result set of a query without executing it.
func (st *statement) ExecuteSchema(ctx context.Context) (schema *arrow.Schema, err error) {
	ctx, span := internal.StartSpan(ctx, "statement.ExecuteSchema", st)
	defer internal.EndSpan(span, err)

	ctx = st.setQueryContext(ctx)

	if st.targetTable != "" {
		err = adbc.Error{
			Msg:  "cannot execute schema for ingestion",
			Code: adbc.StatusInvalidState,
		}
		return nil, err
	}

	if st.query == "" {
		err = adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
		return nil, err
	}

	if st.streamBind != nil || st.bound != nil {
		err = adbc.Error{
			Msg:  "executing schema with bound params not yet implemented",
			Code: adbc.StatusNotImplemented,
		}
		return nil, err
	}

	var loader gosnowflake.ArrowStreamLoader
	loader, err = st.cnxn.cn.QueryArrowStream(gosnowflake.WithDescribeOnly(ctx), st.query)
	if err != nil {
		err = errToAdbcErr(adbc.StatusInternal, err)
		return nil, err
	}

	schema, err = rowTypesToArrowSchema(ctx, loader, st.useHighPrecision, st.maxTimestampPrecision)
	return schema, err
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
func (st *statement) Bind(_ context.Context, values arrow.RecordBatch) error {
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
