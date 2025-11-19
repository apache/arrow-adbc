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

package sqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func getIsolationlevel(lvl sql.IsolationLevel) adbc.OptionIsolationLevel {
	switch lvl {
	case sql.LevelDefault:
		return adbc.LevelDefault
	case sql.LevelReadUncommitted:
		return adbc.LevelReadUncommitted
	case sql.LevelReadCommitted:
		return adbc.LevelReadCommitted
	case sql.LevelRepeatableRead:
		return adbc.LevelRepeatableRead
	case sql.LevelSnapshot:
		return adbc.LevelSnapshot
	case sql.LevelSerializable:
		return adbc.LevelSerializable
	case sql.LevelLinearizable:
		return adbc.LevelLinearizable
	}
	return ""
}

func parseConnectStr(str string) (ret map[string]string, err error) {
	ret = make(map[string]string)
	for _, kv := range strings.Split(str, ";") {
		parsed := strings.SplitN(kv, "=", 2)
		if len(parsed) != 2 {
			return nil, &adbc.Error{
				Msg:  "invalid format for connection string",
				Code: adbc.StatusInvalidArgument,
			}
		}

		ret[strings.TrimSpace(parsed[0])] = strings.TrimSpace(parsed[1])
	}
	return
}

type connector struct {
	db  adbc.Database
	drv adbc.Driver
}

// Connect returns a connection to the database. Connect may
// return a cached connection (one previously closed), but doing
// so is unnecessary; the sql package maintains a pool of idle
// connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes. A default timeout should still be used when
// dialing as a connection pool may call Connect asynchronously
// to any query.
//
// The returned connection is only used by one goroutine at a time.
func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	cnxn, err := c.db.Open(ctx)
	if err != nil {
		return nil, err
	}

	return &conn{Conn: cnxn, drv: c.db}, nil
}

// Driver returns the underlying Driver of the connector,
// mainly to maintain compatibility with the Driver method on sql.DB
func (c *connector) Driver() driver.Driver { return Driver{c.drv} }

// Close closes the underlying database handle that the connector was using.
//
// By implementing the io.Closer interface, sql.DB will correctly call
// Close on the connector when sql.DB.Close is called.
func (c *connector) Close() error {
	return c.db.Close()
}

type Driver struct {
	Driver adbc.Driver
}

// Open returns a new connection to the database. The name
// should be semi-colon separated key-value pairs of the form:
// key=value;key2=value2;.....
//
// Open may return a cached connection (one previously closed),
// but doing so is unnecessary; the sql package maintains a pool
// of idle connections for efficient re-use.
//
// The returned connection is only used by one goroutine at a time.
func (d Driver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector expects the same format as driver.Open
func (d Driver) OpenConnector(name string) (driver.Connector, error) {
	opts, err := parseConnectStr(name)
	if err != nil {
		return nil, err
	}

	db, err := d.Driver.NewDatabase(opts)
	if err != nil {
		return nil, err
	}

	return &connector{db, d.Driver}, nil
}

type ctxOptsKey struct{}

func SetOptionsInCtx(ctx context.Context, opts map[string]string) context.Context {
	return context.WithValue(ctx, ctxOptsKey{}, opts)
}

func GetOptionsFromCtx(ctx context.Context) map[string]string {
	v, ok := ctx.Value(ctxOptsKey{}).(map[string]string)
	if !ok {
		return nil
	}
	return v
}

// conn is a connection to a database. It is not used concurrently by
// multiple goroutines. It is assumed to be stateful.
type conn struct {
	Conn adbc.Connection
	drv  adbc.Database
}

// Close invalidates and potentially stops any current prepared
// statements and transactions, marking this connection as no longer
// in use.
//
// Because the sql package maintains a free pool of connections and
// only calls Close when there's a surplus of idle connections,
// it shouldn't be necessary for drivers to do their own connection
// caching.
//
// Drivers must ensure all network calls made by Close do not block
// indefinitely (e.g. apply a timeout)
func (c *conn) Close() error {
	return c.Conn.Close()
}

func (c *conn) Query(query string, values []driver.Value) (driver.Rows, error) {
	namedValues := make([]driver.NamedValue, len(values))
	for i, value := range values {
		namedValues[i] = driver.NamedValue{
			// nb: Name field is optional
			Ordinal: i,
			Value:   value,
		}
	}
	return c.QueryContext(context.Background(), query, namedValues)
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	s, err := c.Conn.NewStatement()
	if err != nil {
		return nil, err
	}

	if err = s.SetSqlQuery(query); err != nil {
		return nil, errors.Join(err, s.Close())
	}

	return (&stmt{stmt: s}).QueryContext(ctx, args)
}

// Begin exists to fulfill the Conn interface, but will return an error.
// Instead, the ConnBeginTx interface is implemented instead.
//
// Deprecated
func (c *conn) Begin() (driver.Tx, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if postopt, ok := c.Conn.(adbc.PostInitOptions); ok {
		if err := postopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled); err != nil {
			return nil, err
		}
		isolationLevel := getIsolationlevel(sql.IsolationLevel(opts.Isolation))
		if isolationLevel == "" {
			return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
		}

		if err := postopt.SetOption(adbc.OptionKeyIsolationLevel, string(isolationLevel)); err != nil {
			return nil, err
		}

		if opts.ReadOnly {
			if err := postopt.SetOption(adbc.OptionKeyReadOnly, adbc.OptionValueEnabled); err != nil {
				return nil, err
			}
		}
		return tx{ctx: ctx, conn: c.Conn}, nil
	}

	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

// Prepare returns a prepared statement, bound to this connection.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
// Context is for the preparation of the statement. The statement must not
// store the context within the statement itself.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	s, err := c.Conn.NewStatement()
	if err != nil {
		return nil, err
	}

	if err := s.SetSqlQuery(query); err != nil {
		return nil, errors.Join(err, s.Close())
	}

	if err := s.Prepare(ctx); err != nil {
		return nil, errors.Join(err, s.Close())
	}

	paramSchema, err := s.GetParameterSchema()
	var adbcErr adbc.Error
	if errors.As(err, &adbcErr) {
		if adbcErr.Code != adbc.StatusNotImplemented {
			return nil, err
		}
	}

	return &stmt{stmt: s, paramSchema: paramSchema}, nil
}

type tx struct {
	ctx  context.Context
	conn adbc.Connection
}

func (t tx) Commit() error {
	if err := t.conn.Commit(t.ctx); err != nil {
		return err
	}

	return t.conn.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled)
}

func (t tx) Rollback() error {
	if err := t.conn.Rollback(t.ctx); err != nil {
		return err
	}
	return t.conn.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled)
}

type stmt struct {
	stmt        adbc.Statement
	paramSchema *arrow.Schema
}

func (s *stmt) Close() error {
	return s.stmt.Close()
}

func (s *stmt) NumInput() int {
	if s.paramSchema == nil {
		return -1
	}

	return len(s.paramSchema.Fields())
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, driver.ErrSkip
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, driver.ErrSkip
}

func checkType[T any](val any) bool {
	switch val.(type) {
	case T, *T:
	default:
		return false
	}
	return true
}

func isCorrectParamType(typ arrow.Type, val driver.Value) bool {
	switch typ {
	case arrow.BINARY:
		return checkType[[]byte](val)
	case arrow.BOOL:
		return checkType[bool](val)
	case arrow.INT8:
		return checkType[int8](val)
	case arrow.UINT8:
		return checkType[uint8](val)
	case arrow.INT16:
		return checkType[int16](val)
	case arrow.UINT16:
		return checkType[uint16](val)
	case arrow.INT32:
		return checkType[int32](val)
	case arrow.UINT32:
		return checkType[uint32](val)
	case arrow.INT64:
		return checkType[int64](val)
	case arrow.UINT64:
		return checkType[uint64](val)
	case arrow.STRING:
		return checkType[string](val)
	case arrow.FLOAT32:
		return checkType[float32](val)
	case arrow.FLOAT64:
		return checkType[float64](val)
	case arrow.DATE32:
		return checkType[arrow.Date32](val)
	case arrow.DATE64:
		return checkType[arrow.Date64](val)
	case arrow.TIME32:
		return checkType[arrow.Time32](val)
	case arrow.TIME64:
		return checkType[arrow.Time64](val)
	case arrow.TIMESTAMP:
		return checkType[arrow.Timestamp](val)
	case arrow.DECIMAL128:
		return checkType[decimal128.Num](val)
	case arrow.DECIMAL256:
		return checkType[decimal256.Num](val)
	}
	// TODO: add more types here
	return true
}

// this will check the value against the parameter schema if it
// exists, and if the type is non-NA, will enforce the correct type.
func (s *stmt) CheckNamedValue(val *driver.NamedValue) error {
	if s.paramSchema == nil {
		// we don't know the parameter schema, so we can't validate
		// the arguments.
		return driver.ErrSkip
	}

	var field arrow.Field
	if val.Name != "" {
		fields, exists := s.paramSchema.FieldsByName(val.Name)
		if !exists {
			return &adbc.Error{
				Msg:  "could not find parameter named '" + val.Name + "'",
				Code: adbc.StatusInvalidArgument,
			}
		}

		field = fields[0]
	} else {
		if val.Ordinal > len(s.paramSchema.Fields()) {
			return &adbc.Error{
				Msg:  "too many parameters passed for query",
				Code: adbc.StatusInvalidArgument,
			}
		}
		// val.Ordinal is 1-based
		field = s.paramSchema.Fields()[val.Ordinal-1]
	}

	if field.Type.ID() == arrow.NULL {
		return nil
	}

	if !isCorrectParamType(field.Type.ID(), val.Value) {
		return &adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "expected parameter of type " + field.Type.String(),
		}
	}

	return nil
}

func arrFromVal(val any, dt arrow.DataType) (arrow.Array, error) {
	var (
		buffers = make([]*memory.Buffer, 2)
	)
	switch v := val.(type) {
	case bool:
		dt = arrow.FixedWidthTypes.Boolean
		buffers[1] = memory.NewBufferBytes((*[1]byte)(unsafe.Pointer(&v))[:])
	case int8:
		dt = arrow.PrimitiveTypes.Int8
		buffers[1] = memory.NewBufferBytes((*[1]byte)(unsafe.Pointer(&v))[:])
	case uint8:
		dt = arrow.PrimitiveTypes.Uint8
		buffers[1] = memory.NewBufferBytes((*[1]byte)(unsafe.Pointer(&v))[:])
	case int16:
		dt = arrow.PrimitiveTypes.Int16
		buffers[1] = memory.NewBufferBytes((*[2]byte)(unsafe.Pointer(&v))[:])
	case uint16:
		dt = arrow.PrimitiveTypes.Uint16
		buffers[1] = memory.NewBufferBytes((*[2]byte)(unsafe.Pointer(&v))[:])
	case int32:
		dt = arrow.PrimitiveTypes.Int32
		buffers[1] = memory.NewBufferBytes((*[4]byte)(unsafe.Pointer(&v))[:])
	case uint32:
		dt = arrow.PrimitiveTypes.Uint32
		buffers[1] = memory.NewBufferBytes((*[4]byte)(unsafe.Pointer(&v))[:])
	case int64:
		dt = arrow.PrimitiveTypes.Int64
		buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&v))[:])
	case uint64:
		dt = arrow.PrimitiveTypes.Uint64
		buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&v))[:])
	case float32:
		dt = arrow.PrimitiveTypes.Float32
		buffers[1] = memory.NewBufferBytes((*[4]byte)(unsafe.Pointer(&v))[:])
	case float64:
		dt = arrow.PrimitiveTypes.Float64
		buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&v))[:])
	case arrow.Date32:
		dt = arrow.PrimitiveTypes.Date32
		buffers[1] = memory.NewBufferBytes((*[4]byte)(unsafe.Pointer(&v))[:])
	case arrow.Date64:
		dt = arrow.PrimitiveTypes.Date64
		buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&v))[:])
	case []byte:
		if dt == nil || dt.ID() == arrow.BINARY {
			dt = arrow.BinaryTypes.Binary
			buffers[1] = memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, int32(len(v))}))
		} else if dt.ID() == arrow.LARGE_BINARY {
			dt = arrow.BinaryTypes.LargeBinary
			buffers[1] = memory.NewBufferBytes(arrow.Int64Traits.CastToBytes([]int64{0, int64(len(v))}))
		}
		buffers = append(buffers, memory.NewBufferBytes(v))
	case string:
		if dt == nil || dt.ID() == arrow.STRING {
			dt = arrow.BinaryTypes.String
			buffers[1] = memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, int32(len(v))}))
		} else if dt.ID() == arrow.LARGE_STRING {
			dt = arrow.BinaryTypes.LargeString
			buffers[1] = memory.NewBufferBytes(arrow.Int64Traits.CastToBytes([]int64{0, int64(len(v))}))
		}
		buf := unsafe.Slice(unsafe.StringData(v), len(v))
		buffers = append(buffers, memory.NewBufferBytes(buf))
	case arrow.Time32:
		if dt == nil || dt.ID() != arrow.TIME32 {
			return nil, errors.New("can only create array from arrow.Time32 with a provided parameter schema")
		}

		buffers[1] = memory.NewBufferBytes((*[4]byte)(unsafe.Pointer(&v))[:])
	case arrow.Time64:
		if dt == nil || dt.ID() != arrow.TIME64 {
			return nil, errors.New("can only create array from arrow.Time64 with a provided parameter schema")
		}

		buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&v))[:])
	case arrow.Timestamp:
		if dt == nil || dt.ID() != arrow.TIMESTAMP {
			return nil, errors.New("can only create array from arrow.Timestamp with a provided parameter schema")
		}

		buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&v))[:])
	case time.Time:
		if dt == nil {
			return nil, errors.New("can only create array from time.Time with a provided parameter schema")
		}

		switch dt.ID() {
		case arrow.DATE32:
			val := arrow.Date32FromTime(v)
			buffers[1] = memory.NewBufferBytes((*[4]byte)(unsafe.Pointer(&val))[:])
		case arrow.DATE64:
			val := arrow.Date64FromTime(v)
			buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&val))[:])
		case arrow.TIMESTAMP:
			val, err := arrow.TimestampFromTime(v, dt.(*arrow.TimestampType).Unit)
			if err != nil {
				return nil, fmt.Errorf("could not convert time.Time to arrow.Timestamp: %v", err)
			}
			buffers[1] = memory.NewBufferBytes((*[8]byte)(unsafe.Pointer(&val))[:])
		default:
			return nil, fmt.Errorf("time.Time with type %s unsupported", dt)
		}
	default:
		return nil, fmt.Errorf("unsupported type %T", val)
	}
	for _, b := range buffers {
		if b != nil {
			defer b.Release()
		}
	}
	data := array.NewData(dt, 1, buffers, nil, 0, 0)
	defer data.Release()
	return array.MakeFromData(data), nil
}

func createBoundRecord(values []driver.NamedValue, schema *arrow.Schema) (arrow.RecordBatch, error) {
	fields := make([]arrow.Field, len(values))
	cols := make([]arrow.Array, len(values))
	if schema == nil {
		for _, v := range values {
			f := &fields[v.Ordinal-1]
			if v.Name == "" {
				f.Name = strconv.Itoa(v.Ordinal)
			} else {
				f.Name = v.Name
			}
			arr, err := arrFromVal(v.Value, nil)
			if err != nil {
				return nil, err
			}
			defer arr.Release()
			f.Type = arr.DataType()
			cols[v.Ordinal-1] = arr
		}

		return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1), nil
	}

	for _, v := range values {
		var idx int
		var name string
		if v.Name != "" {
			idx = schema.FieldIndices(v.Name)[0]
			name = v.Name
		} else {
			idx = v.Ordinal - 1
			name = strconv.Itoa(idx)
		}

		f := &fields[idx]
		f.Name = name
		arr, err := arrFromVal(v.Value, f.Type)
		if err != nil {
			return nil, err
		}
		defer arr.Release()
		f.Type = arr.DataType()
		cols[idx] = arr
	}
	return array.NewRecordBatch(arrow.NewSchema(fields, nil), cols, 1), nil
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		rec, err := createBoundRecord(args, s.paramSchema)
		if err != nil {
			return nil, err
		}

		if err := s.stmt.Bind(ctx, rec); err != nil {
			return nil, err
		}
	}

	affected, err := s.stmt.ExecuteUpdate(ctx)
	if err != nil {
		return nil, err
	}

	return driver.RowsAffected(affected), nil
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		rec, err := createBoundRecord(args, s.paramSchema)
		if err != nil {
			return nil, err
		}

		if err := s.stmt.Bind(ctx, rec); err != nil {
			return nil, err
		}
	}

	rdr, affected, err := s.stmt.ExecuteQuery(ctx)
	if err != nil {
		return nil, err
	}

	return &rows{rdr: rdr, rowsAffected: affected, stmt: s}, nil
}

type rows struct {
	rdr          array.RecordReader
	curRow       int64
	curRecord    arrow.RecordBatch
	rowsAffected int64
	stmt         *stmt
}

func (r *rows) Columns() (out []string) {
	out = make([]string, len(r.rdr.Schema().Fields()))
	for i, f := range r.rdr.Schema().Fields() {
		out[i] = f.Name
	}
	return
}

func (r *rows) Close() error {
	if r.curRecord != nil {
		r.curRecord = nil
	}

	r.rdr.Release()
	r.rdr = nil

	err := r.stmt.Close()
	r.stmt = nil
	return err
}

func (r *rows) Next(dest []driver.Value) error {
	if r.curRecord != nil && r.curRow == r.curRecord.NumRows() {
		r.curRecord = nil
	}

	for r.curRecord == nil {
		if !r.rdr.Next() {
			if err := r.rdr.Err(); err != nil {
				return err
			}
			return io.EOF
		}
		r.curRecord = r.rdr.RecordBatch()
		r.curRow = 0
		if r.curRecord.NumRows() == 0 {
			r.curRecord = nil
		}
	}

	for i, col := range r.curRecord.Columns() {
		if col.IsNull(int(r.curRow)) {
			dest[i] = nil
			continue
		}
		if colUnion, ok := col.(array.Union); ok {
			col = colUnion.Field(colUnion.ChildID(int(r.curRow)))
		}
		switch col := col.(type) {
		case *array.Boolean:
			dest[i] = col.Value(int(r.curRow))
		case *array.Int8:
			dest[i] = col.Value(int(r.curRow))
		case *array.Uint8:
			dest[i] = col.Value(int(r.curRow))
		case *array.Int16:
			dest[i] = col.Value(int(r.curRow))
		case *array.Uint16:
			dest[i] = col.Value(int(r.curRow))
		case *array.Int32:
			dest[i] = col.Value(int(r.curRow))
		case *array.Uint32:
			dest[i] = col.Value(int(r.curRow))
		case *array.Int64:
			dest[i] = col.Value(int(r.curRow))
		case *array.Uint64:
			dest[i] = col.Value(int(r.curRow))
		case *array.Float32:
			dest[i] = col.Value(int(r.curRow))
		case *array.Float64:
			dest[i] = col.Value(int(r.curRow))
		case *array.String:
			dest[i] = col.Value(int(r.curRow))
		case *array.LargeString:
			dest[i] = col.Value(int(r.curRow))
		case *array.Binary:
			dest[i] = col.Value(int(r.curRow))
		case *array.LargeBinary:
			dest[i] = col.Value(int(r.curRow))
		case *array.Date32:
			dest[i] = col.Value(int(r.curRow)).ToTime()
		case *array.Date64:
			dest[i] = col.Value(int(r.curRow)).ToTime()
		case *array.Time32:
			dest[i] = col.Value(int(r.curRow)).ToTime(col.DataType().(*arrow.Time32Type).Unit)
		case *array.Time64:
			dest[i] = col.Value(int(r.curRow)).ToTime(col.DataType().(*arrow.Time64Type).Unit)
		case *array.Timestamp:
			dest[i] = col.Value(int(r.curRow)).ToTime(col.DataType().(*arrow.TimestampType).Unit)
		case *array.Decimal128:
			dest[i] = col.Value(int(r.curRow))
		case *array.Decimal256:
			dest[i] = col.Value(int(r.curRow))
		default:
			return &adbc.Error{
				Code: adbc.StatusNotImplemented,
				Msg:  "not yet implemented populating from columns of type " + col.DataType().String(),
			}
		}
	}

	r.curRow++
	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.rdr.Schema().Field(index).Type.String()
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	return r.rdr.Schema().Field(index).Nullable, true
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	typ := r.rdr.Schema().Field(index).Type
	switch dt := typ.(type) {
	case *arrow.Decimal128Type:
		return int64(dt.Precision), int64(dt.Scale), true
	case *arrow.Decimal256Type:
		return int64(dt.Precision), int64(dt.Scale), true
	}
	return 0, 0, false
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	switch r.rdr.Schema().Field(index).Type.ID() {
	case arrow.BOOL:
		return reflect.TypeOf(false)
	case arrow.INT8:
		return reflect.TypeOf(int8(0))
	case arrow.UINT8:
		return reflect.TypeOf(uint8(0))
	case arrow.INT16:
		return reflect.TypeOf(int16(0))
	case arrow.UINT16:
		return reflect.TypeOf(uint16(0))
	case arrow.INT32:
		return reflect.TypeOf(int32(0))
	case arrow.UINT32:
		return reflect.TypeOf(uint32(0))
	case arrow.INT64:
		return reflect.TypeOf(int64(0))
	case arrow.UINT64:
		return reflect.TypeOf(uint64(0))
	case arrow.FLOAT32:
		return reflect.TypeOf(float32(0))
	case arrow.FLOAT64:
		return reflect.TypeOf(float64(0))
	case arrow.DECIMAL128:
		return reflect.TypeOf(decimal128.Num{})
	case arrow.DECIMAL256:
		return reflect.TypeOf(decimal256.Num{})
	case arrow.BINARY:
		return reflect.TypeOf([]byte{})
	case arrow.STRING:
		return reflect.TypeOf(string(""))
	case arrow.TIME32, arrow.TIME64, arrow.DATE32, arrow.DATE64, arrow.TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	}
	return nil
}
