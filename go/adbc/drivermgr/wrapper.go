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
// #include "adbc.h"
// #include <stdlib.h>
//
// void releaseErr(struct AdbcError* err) { err->release(err); }
//
import "C"
import (
	"context"
	"runtime"
	"unsafe"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/cdata"
)

type option struct {
	key, val *C.char
}

type Driver struct {
	options map[string]option
	db      *C.struct_AdbcDatabase
}

func toAdbcError(code adbc.Status, e *C.struct_AdbcError) error {
	err := &adbc.Error{
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

func (d *Driver) SetOptions(options map[string]string) error {
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

func (d *Driver) Open(context.Context) (adbc.Connection, error) {
	var err C.struct_AdbcError
	if d.db == nil {
		d.db = (*C.struct_AdbcDatabase)(unsafe.Pointer(C.malloc(C.sizeof_struct_AdbcDatabase)))
		if code := adbc.Status(C.AdbcDatabaseNew(d.db, &err)); code != adbc.StatusOK {
			return nil, toAdbcError(code, &err)
		}

		for _, o := range d.options {
			if code := adbc.Status(C.AdbcDatabaseSetOption(d.db, o.key, o.val, &err)); code != adbc.StatusOK {
				errOut := toAdbcError(code, &err)
				C.AdbcDatabaseRelease(d.db, &err)
				d.db = nil
				return nil, errOut
			}
		}

		if code := adbc.Status(C.AdbcDatabaseInit(d.db, &err)); code != adbc.StatusOK {
			errOut := toAdbcError(code, &err)
			C.AdbcDatabaseRelease(d.db, &err)
			d.db = nil
			return nil, errOut
		}

		runtime.SetFinalizer(d, func(drv *Driver) {
			if drv.db != nil {
				var err C.struct_AdbcError
				code := adbc.Status(C.AdbcDatabaseRelease(drv.db, &err))
				if code != adbc.StatusOK {
					panic(toAdbcError(code, &err))
				}
			}
		})
	}

	var c C.struct_AdbcConnection
	if code := adbc.Status(C.AdbcConnectionNew(&c, &err)); code != adbc.StatusOK {
		return nil, toAdbcError(code, &err)
	}

	if code := adbc.Status(C.AdbcConnectionInit(&c, d.db, &err)); code != adbc.StatusOK {
		errOut := toAdbcError(code, &err)
		C.AdbcConnectionRelease(&c, &err)
		return nil, errOut
	}

	return &cnxn{conn: &c}, nil
}

func getRdr(out *C.struct_ArrowArrayStream) array.RecordReader {
	return cdata.ImportCArrayStream((*cdata.CArrowArrayStream)(unsafe.Pointer(out)), nil).(array.RecordReader)
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

	return getRdr(&out), nil
}

func (c *cnxn) GetObjects(_ context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) GetTableSchema(_ context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) GetTableTypes(context.Context) (array.RecordReader, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) Commit(context.Context) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) Rollback(context.Context) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
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
}

func (s *stmt) Close() error {
	var err C.struct_AdbcError
	if code := adbc.Status(C.AdbcStatementRelease(s.st, &err)); code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
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

func (s *stmt) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	var (
		out      C.struct_ArrowArrayStream
		affected C.int64_t
		err      C.struct_AdbcError
	)
	code := adbc.Status(C.AdbcStatementExecuteQuery(s.st, &out, &affected, &err))
	if code != adbc.StatusOK {
		return nil, 0, toAdbcError(code, &err)
	}

	return getRdr(&out), int64(affected), nil
}

func (s *stmt) ExecuteUpdate(context.Context) (int64, error) {
	var (
		nrows C.int64_t
		err   C.struct_AdbcError
	)
	if code := adbc.Status(C.AdbcStatementExecuteQuery(s.st, nil, &nrows, &err)); code != adbc.StatusOK {
		return -1, toAdbcError(code, &err)
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

func (s *stmt) Bind(_ context.Context, values arrow.Record) error {
	var (
		arr    cdata.CArrowArray
		schema cdata.CArrowSchema
		err    C.struct_AdbcError
	)
	cdata.ExportArrowRecordBatch(values, &arr, &schema)

	code := adbc.Status(C.AdbcStatementBind(s.st, (*C.struct_ArrowArray)(unsafe.Pointer(&arr)), (*C.struct_ArrowSchema)(unsafe.Pointer(&schema)), &err))
	if code != adbc.StatusOK {
		return toAdbcError(code, &err)
	}
	return nil
}

func (s *stmt) BindStream(_ context.Context, stream array.RecordReader) error {
	return &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *stmt) GetParameterSchema() (*arrow.Schema, error) {
	return nil, &adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *stmt) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, &adbc.Error{Code: adbc.StatusNotImplemented}
}
