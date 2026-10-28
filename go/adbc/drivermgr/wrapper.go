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

package drivermgr

// #cgo !windows LDFLAGS: -ldl
// #cgo windows CFLAGS: -DADBC_EXPORTING
// #cgo windows CPPFLAGS: -DADBC_EXPORTING
// #cgo windows LDFLAGS: -lshell32 -ladvapi32 -luuid
// #cgo CXXFLAGS: -std=c++17 -I${SRCDIR}/vendored/
// #if !defined(ADBC_EXPORTING)
// #define ADBC_EXPORTING
// #endif
// #include "arrow-adbc/adbc.h"
// #include "arrow-adbc/adbc_driver_manager.h"
// #include <stdlib.h>
// #include <string.h>
//
// void releaseErr(struct AdbcError* err) {
//     if (err->release != NULL) {
//         err->release(err);
//         err->release = NULL;
//     }
// }
// struct ArrowArray* allocArr() {
//     struct ArrowArray* array = (struct ArrowArray*)malloc(sizeof(struct ArrowArray));
//     memset(array, 0, sizeof(struct ArrowArray));
//     return array;
// }
//
// struct ArrowArrayStream* allocArrStream() {
//     struct ArrowArrayStream* stream = (struct ArrowArrayStream*)malloc(sizeof(struct ArrowArrayStream));
//     memset(stream, 0, sizeof(struct ArrowArrayStream));
//     return stream;
// }
//
import "C"
import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/cdata"
)

const (
	LoadFlagsSearchEnv = 1 << iota
	LoadFlagsSearchPath
	LoadFlagsSearchSystem
	LoadFlagsAllowRelativePaths

	LoadFlagsDefault = LoadFlagsSearchEnv | LoadFlagsSearchPath | LoadFlagsSearchSystem | LoadFlagsAllowRelativePaths
	// LoadFlagsOptionKey is the key to use for an option to set specific
	// load flags for the database to decide where to look for driver manifests.
	LoadFlagsOptionKey = "load_flags"
)

type option struct {
	key, val *C.char
}

func convOptions(incoming map[string]string, existing map[string]option) {
	for k, v := range incoming {
		o, ok := existing[k]
		if !ok {
			o.key = C.CString(k)
			o.val = C.CString(v)
			existing[k] = o
			continue
		}

		C.free(unsafe.Pointer(o.val))
		o.val = C.CString(v)
		existing[k] = o
	}
}

type Driver struct{}

func (d Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d Driver) NewDatabaseWithContext(_ context.Context, opts map[string]string) (adbc.Database, error) {
	dbOptions := make(map[string]option)
	convOptions(opts, dbOptions)

	db := &Database{
		options: make(map[string]option),
	}

	defer func() {
		if db.db == nil { // cleanup options if we failed to create the database
			for _, o := range dbOptions {
				C.free(unsafe.Pointer(o.key))
				C.free(unsafe.Pointer(o.val))
			}
		}
	}()

	var err C.struct_AdbcError
	db.db = (*C.struct_AdbcDatabase)(unsafe.Pointer(C.calloc(C.sizeof_struct_AdbcDatabase, C.size_t(1))))
	if code := adbc.Status(C.AdbcDatabaseNew(db.db, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}

	if code := adbc.Status(C.AdbcDriverManagerDatabaseSetLoadFlags(db.db, C.AdbcLoadFlags(LoadFlagsDefault), &err)); code != adbc.StatusOK {
		errOut := toAdbcError(code, &err)
		C.AdbcDatabaseRelease(db.db, &err)
		db.db = nil
		return nil, errOut
	}

	for k, o := range dbOptions {
		switch k {
		case LoadFlagsOptionKey:
			f, errOut := strconv.Atoi(C.GoString(o.val))
			if errOut != nil {
				C.AdbcDatabaseRelease(db.db, &err)
				db.db = nil
				return nil, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "invalid load flags value: " + C.GoString(o.val),
				}
			}

			if code := adbc.Status(C.AdbcDriverManagerDatabaseSetLoadFlags(db.db, C.AdbcLoadFlags(f), &err)); code != adbc.StatusOK {
				errOut := toAdbcError(code, &err)
				C.AdbcDatabaseRelease(db.db, &err)
				db.db = nil
				return nil, errOut
			}
		default:
			if code := adbc.Status(C.AdbcDatabaseSetOption(db.db, o.key, o.val, &err)); code != adbc.StatusOK {
				errOut := toAdbcError(code, &err)
				C.AdbcDatabaseRelease(db.db, &err)
				db.db = nil
				return nil, errOut
			}
		}
	}

	if code := adbc.Status(C.AdbcDatabaseInit(db.db, &err)); code != adbc.StatusOK {
		errOut := toAdbcError(code, &err)
		C.AdbcDatabaseRelease(db.db, &err)
		db.db = nil
		return nil, errOut
	}

	return db, nil
}

type Database struct {
	options map[string]option
	db      *C.struct_AdbcDatabase

	mu     sync.Mutex // protects following fields
	closed bool
}

func toAdbcError(code adbc.Status, e *C.struct_AdbcError) error {
	if e == nil || e.release == nil {
		return adbc.Error{
			Code: code,
			Msg:  "[drivermgr] nil error",
		}
	}
	err := adbc.Error{
		Code:       code,
		VendorCode: int32(e.vendor_code),
		Msg:        C.GoString(e.message),
	}
	for i := 0; i < 5; i++ {
		err.SqlState[i] = byte(e.sqlstate[i])
	}
	C.releaseErr(e)
	return err
}

func contextAdbcError(ctx context.Context) error {
	switch ctx.Err() {
	case context.Canceled:
		return adbc.Error{Code: adbc.StatusCancelled, Msg: context.Canceled.Error()}
	case context.DeadlineExceeded:
		return adbc.Error{Code: adbc.StatusTimeout, Msg: context.DeadlineExceeded.Error()}
	default:
		return nil
	}
}

func contextDriverError(ctx context.Context, driverErr error) error {
	ctxErr := ctx.Err()
	if ctxErr == nil {
		return driverErr
	}

	status := adbc.StatusCancelled
	if ctxErr == context.DeadlineExceeded {
		status = adbc.StatusTimeout
	}
	var adbcErr adbc.Error
	if errors.As(driverErr, &adbcErr) {
		if adbcErr.Code == adbc.StatusCancelled || adbcErr.Code == status {
			return driverErr
		}
		adbcErr.Code = status
		adbcErr.Msg = ctxErr.Error() + ": " + adbcErr.Msg
		return adbcErr
	}
	return adbc.Error{Code: status, Msg: ctxErr.Error() + ": " + driverErr.Error()}
}

func (d *Database) SetOptions(options map[string]string) error {
	if d.options == nil {
		d.options = make(map[string]option)
	}

	for k, v := range options {
		o, ok := d.options[k]
		if !ok {
			o.key = C.CString(k)
			o.val = C.CString(v)
			d.options[k] = o
			continue
		}

		C.free(unsafe.Pointer(o.val))
		o.val = C.CString(v)
		d.options[k] = o
	}
	return nil
}

func (d *Database) Open(context.Context) (adbc.Connection, error) {
	var err C.struct_AdbcError

	var c C.struct_AdbcConnection
	if code := adbc.Status(C.AdbcConnectionNew(&c, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}

	for _, o := range d.options {
		if code := adbc.Status(C.AdbcConnectionSetOption(&c, o.key, o.val, &err)); code != adbc.StatusOK {
			errOut := toAdbcError(code, &err)
			C.AdbcConnectionRelease(&c, &err)
			return nil, errOut
		}
	}

	if code := adbc.Status(C.AdbcConnectionInit(&c, d.db, &err)); code != adbc.StatusOK {
		errOut := toAdbcError(code, &err)
		C.AdbcConnectionRelease(&c, &err)
		return nil, errOut
	}

	return &cnxn{conn: &c}, nil
}

func (d *Database) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.closed {
		return nil
	}

	d.closed = true

	for _, o := range d.options {
		C.free(unsafe.Pointer(o.key))
		C.free(unsafe.Pointer(o.val))
	}

	if d.db != nil {
		var err C.struct_AdbcError
		code := adbc.Status(C.AdbcDatabaseRelease(d.db, &err))
		if code != adbc.StatusOK {
			return toAdbcError(code, &err)
		}
	}

	return nil
}

func getRdr(out *C.struct_ArrowArrayStream) (array.RecordReader, error) {
	rdr, err := cdata.ImportCRecordReader((*cdata.CArrowArrayStream)(unsafe.Pointer(out)), nil)
	if err != nil {
		return nil, err
	}
	return rdr.(array.RecordReader), nil
}

func getSchema(out *C.struct_ArrowSchema) (*arrow.Schema, error) {
	// Maybe: ImportCArrowSchema should perform this check?
	if out.format == nil {
		return nil, nil
	}

	return cdata.ImportCArrowSchema((*cdata.CArrowSchema)(unsafe.Pointer(out)))
}

type cnxn struct {
	conn *C.struct_AdbcConnection
}

func (c *cnxn) GetInfo(_ context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	var (
		out   C.struct_ArrowArrayStream
		err   C.struct_AdbcError
		codes *C.uint32_t
	)
	if len(infoCodes) > 0 {
		codes = (*C.uint32_t)(unsafe.Pointer(&infoCodes[0]))
	}

	if code := adbc.Status(C.AdbcConnectionGetInfo(c.conn, codes, (C.size_t)(len(infoCodes)), &out, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}

	return getRdr(&out)
}

func (c *cnxn) GetObjects(_ context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	var (
		out         C.struct_ArrowArrayStream
		err         C.struct_AdbcError
		catalog_    *C.char
		dbSchema_   *C.char
		tableName_  *C.char
		columnName_ *C.char
		tableType_  **C.char
	)

	if catalog != nil {
		catalog_ = C.CString(*catalog)
		defer C.free(unsafe.Pointer(catalog_))
	}

	if dbSchema != nil {
		dbSchema_ = C.CString(*dbSchema)
		defer C.free(unsafe.Pointer(dbSchema_))
	}

	if tableName != nil {
		tableName_ = C.CString(*tableName)
		defer C.free(unsafe.Pointer(tableName_))
	}

	if columnName != nil {
		columnName_ = C.CString(*columnName)
		defer C.free(unsafe.Pointer(columnName_))
	}

	if len(tableType) > 0 {
		cArr := make([]*C.char, len(tableType)+1)
		for i, tt := range tableType {
			cs := C.CString(tt)
			cArr[i] = cs
			defer C.free(unsafe.Pointer(cs))
		}
		tableType_ = &cArr[0]
	}

	if code := adbc.Status(C.AdbcConnectionGetObjects(c.conn, C.int(depth), catalog_, dbSchema_, tableName_, tableType_, columnName_, &out, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}
	return getRdr(&out)
}

func (c *cnxn) GetTableSchema(_ context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	var (
		schema     C.struct_ArrowSchema
		err        C.struct_AdbcError
		catalog_   *C.char
		dbSchema_  *C.char
		tableName_ *C.char
	)

	if catalog != nil {
		catalog_ = C.CString(*catalog)
		defer C.free(unsafe.Pointer(catalog_))
	}

	if dbSchema != nil {
		dbSchema_ = C.CString(*dbSchema)
		defer C.free(unsafe.Pointer(dbSchema_))
	}

	tableName_ = C.CString(tableName)
	defer C.free(unsafe.Pointer(tableName_))

	if code := adbc.Status(C.AdbcConnectionGetTableSchema(c.conn, catalog_, dbSchema_, tableName_, &schema, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}

	return getSchema(&schema)
}

func (c *cnxn) GetTableTypes(context.Context) (array.RecordReader, error) {
	var (
		out C.struct_ArrowArrayStream
		err C.struct_AdbcError
	)

	if code := adbc.Status(C.AdbcConnectionGetTableTypes(c.conn, &out, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}
	return getRdr(&out)
}

func (c *cnxn) Commit(context.Context) error {
	var (
		err C.struct_AdbcError
	)

	if code := adbc.Status(C.AdbcConnectionCommit(c.conn, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}

	return nil
}

func (c *cnxn) Rollback(context.Context) error {
	var (
		err C.struct_AdbcError
	)

	if code := adbc.Status(C.AdbcConnectionRollback(c.conn, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}

	return nil
}

func (c *cnxn) NewStatement() (adbc.Statement, error) {
	var st C.struct_AdbcStatement
	var err C.struct_AdbcError
	if code := adbc.Status(C.AdbcStatementNew(c.conn, &st, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}

	return &stmt{st: &st}, nil
}

func (c *cnxn) Close() error {
	var err C.struct_AdbcError
	if code := adbc.Status(C.AdbcConnectionRelease(c.conn, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

func (c *cnxn) ReadPartition(_ context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) SetOption(key, value string) error {
	ckey, cvalue := C.CString(key), C.CString(value)
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cvalue))

	var err C.struct_AdbcError
	if code := adbc.Status(C.AdbcConnectionSetOption(c.conn, ckey, cvalue, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

type stmt struct {
	st *C.struct_AdbcStatement

	mu     sync.Mutex // serializes cancellation and release
	closed bool
	active *cancellationWatch
}

type cancellationWatch struct {
	stop func() bool
	done chan struct{}
	once sync.Once
}

func (w *cancellationWatch) stopAndJoin() {
	w.once.Do(func() {
		// AfterFunc's stop does not wait for an already-started callback.
		if !w.stop() {
			<-w.done
		}
	})
}

func (s *stmt) stopActiveWatch() {
	s.mu.Lock()
	old := s.active
	s.active = nil
	s.mu.Unlock()
	if old != nil {
		old.stopAndJoin()
	}
}

func (s *stmt) stopWatching(w *cancellationWatch) {
	if w == nil {
		return
	}
	s.mu.Lock()
	if s.active == w {
		s.active = nil
	}
	s.mu.Unlock()
	w.stopAndJoin()
}

func (s *stmt) watchCancellation(ctx context.Context) *cancellationWatch {
	if ctx.Done() == nil {
		return nil
	}

	w := &cancellationWatch{done: make(chan struct{})}
	s.mu.Lock()
	w.stop = context.AfterFunc(ctx, func() {
		s.mu.Lock()
		if !s.closed && s.active == w {
			var err C.struct_AdbcError
			C.AdbcStatementCancel(s.st, &err)
			C.releaseErr(&err) // Cancellation is best-effort, including NOT_IMPLEMENTED.
		}
		s.mu.Unlock()
		close(w.done)
	})
	s.active = w
	s.mu.Unlock()
	return w
}

func (s *stmt) Close() error {
	s.stopActiveWatch()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}

	var err C.struct_AdbcError
	s.closed = true // The driver manager invalidates the statement even if release fails.
	if code := adbc.Status(C.AdbcStatementRelease(s.st, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

type cancellationReader struct {
	array.RecordReader
	ctx   context.Context
	stmt  *stmt
	watch *cancellationWatch
	refs  atomic.Int64
	err   error
}

func (r *cancellationReader) stopWatching() {
	r.stmt.stopWatching(r.watch)
}
func (r *cancellationReader) Retain() {
	r.RecordReader.Retain()
	r.refs.Add(1)
}

func (r *cancellationReader) Release() {
	if r.refs.Add(-1) == 0 {
		r.stopWatching()
	}
	r.RecordReader.Release()
}

func (r *cancellationReader) Next() bool {
	if err := contextAdbcError(r.ctx); err != nil {
		r.err = err
		// Leave the watcher armed until Release: it must still cancel the
		// statement if the callback has not started yet.
		return false
	}

	ok := r.RecordReader.Next()
	if err := contextAdbcError(r.ctx); err != nil {
		if !ok {
			if driverErr := r.RecordReader.Err(); driverErr != nil {
				err = contextDriverError(r.ctx, driverErr)
			}
			r.stopWatching()
		}
		r.err = err
		return false
	}
	if !ok {
		r.stopWatching()
	}
	return ok
}

func (r *cancellationReader) Err() error {
	if r.err != nil {
		return r.err
	}
	return r.RecordReader.Err()
}

func (s *stmt) SetOption(key, val string) error {
	ckey, cvalue := C.CString(key), C.CString(val)
	defer C.free(unsafe.Pointer(ckey))
	defer C.free(unsafe.Pointer(cvalue))

	var err C.struct_AdbcError
	if code := adbc.Status(C.AdbcStatementSetOption(s.st, ckey, cvalue, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

func (s *stmt) SetSqlQuery(query string) error {
	var err C.struct_AdbcError
	cquery := C.CString(query)
	defer C.free(unsafe.Pointer(cquery))

	if code := adbc.Status(C.AdbcStatementSetSqlQuery(s.st, cquery, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

// ExecuteQuery keeps the execution context active until the reader is released
// or exhausted, since drivers may block while fetching from the result stream.
func (s *stmt) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	s.stopActiveWatch()
	if err := contextAdbcError(ctx); err != nil {
		return nil, 0, err
	}

	var (
		out      C.struct_ArrowArrayStream
		affected C.int64_t
		err      C.struct_AdbcError
	)
	watch := s.watchCancellation(ctx)
	if ctxErr := contextAdbcError(ctx); ctxErr != nil {
		s.stopWatching(watch)
		return nil, 0, ctxErr
	}
	code := adbc.Status(C.AdbcStatementExecuteQuery(s.st, &out, &affected, &err))
	if code != adbc.StatusOK {
		s.stopWatching(watch)
		if out.release != nil {
			cdata.ReleaseCArrowArrayStream((*cdata.CArrowArrayStream)(unsafe.Pointer(&out)))
		}
		return nil, 0, contextDriverError(ctx, toAdbcError(code, &err))
	}

	rdr, goerr := getRdr(&out)
	if goerr != nil {
		s.stopWatching(watch)
		return nil, int64(affected), goerr
	}
	if watch == nil {
		return rdr, int64(affected), nil
	}
	wrapped := &cancellationReader{RecordReader: rdr, ctx: ctx, stmt: s, watch: watch}
	wrapped.refs.Store(1)
	return wrapped, int64(affected), nil
}

func (s *stmt) ExecuteUpdate(ctx context.Context) (int64, error) {
	s.stopActiveWatch()
	if err := contextAdbcError(ctx); err != nil {
		return -1, err
	}

	var (
		nrows C.int64_t
		err   C.struct_AdbcError
	)
	watch := s.watchCancellation(ctx)
	if ctxErr := contextAdbcError(ctx); ctxErr != nil {
		s.stopWatching(watch)
		return -1, ctxErr
	}
	code := adbc.Status(C.AdbcStatementExecuteQuery(s.st, nil, &nrows, &err))
	s.stopWatching(watch)
	if code != adbc.StatusOK {
		return -1, contextDriverError(ctx, toAdbcError(code, &err))
	}
	return int64(nrows), nil
}

func (s *stmt) Prepare(context.Context) error {
	var err C.struct_AdbcError
	if code := adbc.Status(C.AdbcStatementPrepare(s.st, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

func (s *stmt) SetSubstraitPlan(plan []byte) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *stmt) Bind(_ context.Context, values arrow.RecordBatch) error {
	var (
		arr    = C.allocArr()
		schema C.struct_ArrowSchema

		cdArr    = (*cdata.CArrowArray)(unsafe.Pointer(arr))
		cdSchema = (*cdata.CArrowSchema)(unsafe.Pointer(&schema))
		err      C.struct_AdbcError
	)
	cdata.ExportArrowRecordBatch(values, cdArr, cdSchema)
	defer func() {
		cdata.ReleaseCArrowArray(cdArr)
		cdata.ReleaseCArrowSchema(cdSchema)

		C.free(unsafe.Pointer(arr))
	}()

	code := adbc.Status(C.AdbcStatementBind(s.st, arr, &schema, &err))
	if code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

func (s *stmt) BindStream(_ context.Context, stream array.RecordReader) error {
	var (
		arrStream   = C.allocArrStream()
		cdArrStream = (*cdata.CArrowArrayStream)(unsafe.Pointer(arrStream))
		err         C.struct_AdbcError
	)
	cdata.ExportRecordReader(stream, cdArrStream)
	if code := adbc.Status(C.AdbcStatementBindStream(s.st, arrStream, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

func (s *stmt) GetParameterSchema() (*arrow.Schema, error) {
	var (
		schema C.struct_ArrowSchema
		err    C.struct_AdbcError
	)

	if code := adbc.Status(C.AdbcStatementGetParameterSchema(s.st, &schema, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}

	return getSchema(&schema)
}

func (s *stmt) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, &adbc.Error{Code: adbc.StatusNotImplemented}
}
