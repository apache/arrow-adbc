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

// Package sqldriver is a wrapper around the ADBC (Arrow Database
// Connectivity) interfaces to support the standard golang database/sql
// package.
//
// This allows any ADBC driver implementation to also be used as-is
// with the database/sql package of the standard library rather than
// having to implement drivers for both separately.
//
// Registering the driver can be done by importing this and then running
//
// 		sql.Register("drivername", sqldriver.Driver{adbcdriver})
//
// EXPERIMENTAL. The ADBC interfaces are subject to change and as such
// this wrapper is also subject to change based on that.
package sqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/decimal128"
	"github.com/apache/arrow/go/v10/arrow/decimal256"
	"github.com/apache/arrow/go/v10/arrow/memory"
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
		parsed := strings.Split(kv, "=")
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
	driver adbc.Driver
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
	cnxn, err := c.driver.Open(ctx)
	if err != nil {
		return nil, err
	}

	return &conn{Conn: cnxn, drv: c.driver}, nil
}

// Driver returns the underlying Driver of the connector,
// mainly to maintain compatibility with the Driver method on sql.DB
func (c *connector) Driver() driver.Driver { return &Driver{c.driver} }

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
	opts, err := parseConnectStr(name)
	if err != nil {
		return nil, err
	}
	err = d.Driver.SetOptions(opts)
	if err != nil {
		return nil, err
	}

	cnxn, err := d.Driver.Open(context.Background())
	if err != nil {
		return nil, err
	}

	return &conn{Conn: cnxn, drv: d.Driver}, nil
}

// OpenConnector expects the same format as driver.Open
func (d Driver) OpenConnector(name string) (driver.Connector, error) {
	opts, err := parseConnectStr(name)
	if err != nil {
		return nil, err
	}
	err = d.Driver.SetOptions(opts)
	if err != nil {
		return nil, err
	}

	return &connector{d.Driver}, nil
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
	drv  adbc.Driver
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
		s.Close()
		return nil, err
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

func arrFromVal(val any) arrow.Array {
	var (
		buffers = make([]*memory.Buffer, 2)
		dt      arrow.DataType
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
		dt = arrow.BinaryTypes.Binary
		buffers[1] = memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, int32(len(v))}))
		buffers[2] = memory.NewBufferBytes(v)
	case string:
		dt = arrow.BinaryTypes.String
		buffers[1] = memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, int32(len(v))}))
		var buf = *(*[]byte)(unsafe.Pointer(&v))
		(*reflect.SliceHeader)(unsafe.Pointer(&buf)).Cap = len(v)
		buffers[2] = memory.NewBufferBytes(buf)
	}
	for _, b := range buffers {
		defer b.Release()
	}
	data := array.NewData(dt, 1, buffers, nil, 0, 0)
	defer data.Release()
	return array.MakeFromData(data)
}

func createBoundRecord(values []driver.NamedValue, schema *arrow.Schema) arrow.Record {
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
			arr := arrFromVal(v.Value)
			defer arr.Release()
			f.Type = arr.DataType()
			cols[v.Ordinal-1] = arr
		}

		return array.NewRecord(arrow.NewSchema(fields, nil), cols, 1)
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
		arr := arrFromVal(v.Value)
		defer arr.Release()
		f.Type = arr.DataType()
		cols[idx] = arr
	}
	return array.NewRecord(arrow.NewSchema(fields, nil), cols, 1)
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > 0 {
		if err := s.stmt.Bind(ctx, createBoundRecord(args, s.paramSchema)); err != nil {
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
		if err := s.stmt.Bind(ctx, createBoundRecord(args, s.paramSchema)); err != nil {
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
	curRecord    arrow.Record
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
	r.stmt = nil
	return nil
}

func (r *rows) Next(dest []driver.Value) error {
	if r.curRecord != nil && r.curRow == r.curRecord.NumRows() {
		r.curRecord = nil
	}

	for r.curRecord == nil {
		if !r.rdr.Next() {
			return io.EOF
		}
		r.curRecord = r.rdr.Record()
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
			dest[i] = col.Value(int(r.curRow))
		case *array.Date64:
			dest[i] = col.Value(int(r.curRow))
		case *array.Time32:
			dest[i] = col.Value(int(r.curRow))
		case *array.Time64:
			dest[i] = col.Value(int(r.curRow))
		case *array.Timestamp:
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
	case arrow.TIME32:
		return reflect.TypeOf(arrow.Time32(0))
	case arrow.TIME64:
		return reflect.TypeOf(arrow.Time64(0))
	case arrow.DATE32:
		return reflect.TypeOf(arrow.Date32(0))
	case arrow.DATE64:
		return reflect.TypeOf(arrow.Date64(0))
	case arrow.TIMESTAMP:
		return reflect.TypeOf(arrow.Timestamp(0))
	}
	return nil
}
