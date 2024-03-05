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

package driverbase_test

import (
	"context"
	"testing"

	"golang.org/x/exp/slog"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	OptionKeyRecognized   = "recognized"
	OptionKeyUnrecognized = "unrecognized"
)

type MockedHandler struct {
	mock.Mock
}

func (h *MockedHandler) Enabled(ctx context.Context, level slog.Level) bool { return true }
func (h *MockedHandler) WithAttrs(attrs []slog.Attr) slog.Handler           { return h }
func (h *MockedHandler) WithGroup(name string) slog.Handler                 { return h }
func (h *MockedHandler) Handle(ctx context.Context, r slog.Record) error {
	// We only care to assert the message value, and want to isolate nondetermistic behavior (e.g. timestamp)
	args := h.Called(ctx, r.Message)
	return args.Error(0)
}

func NewDriver(alloc memory.Allocator, m *mock.Mock) adbc.Driver {
	info := driverbase.DefaultDriverInfo("MockDriver")
	_ = info.RegisterInfoCode(adbc.InfoCode(10_001), "my custom info")
	return driverbase.NewDriver(&driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc), mock: m})
}

func TestDriver(t *testing.T) {
	var (
		m         mock.Mock
		dbHandler MockedHandler
	)
	dbHandler.On("Handle", mock.Anything, "only db can say this").Return(nil)

	ctx := context.TODO()
	alloc := memory.DefaultAllocator
	drv := NewDriver(alloc, &m)

	db, err := drv.NewDatabase(nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.SetOptions(map[string]string{OptionKeyRecognized: "should-pass"}))

	err = db.SetOptions(map[string]string{OptionKeyUnrecognized: "should-fail"})
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unknown database option 'unrecognized'", err.Error())

	// Setup a logger for the database
	db.(driverbase.Database).SetLogger(slog.New(&dbHandler))

	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	defer cnxn.Close()

	err = cnxn.Commit(ctx)
	require.Error(t, err)
	require.Equal(t, "Invalid State: [MockDriver] Cannot commit when autocommit is enabled", err.Error())

	err = cnxn.Rollback(ctx)
	require.Error(t, err)
	require.Equal(t, "Invalid State: [MockDriver] Cannot rollback when autocommit is enabled", err.Error())

	info, err := cnxn.GetInfo(ctx, nil)
	require.NoError(t, err)

	// This is what the driverbase provided GetInfo result should look like out of the box,
	// with one custom setting registered at initialization
	expectedGetInfoTable, err := array.TableFromJSON(alloc, adbc.GetInfoSchema, []string{`[
		{
			"info_name": 0,
			"info_value": [0, "MockDriver"]
		},
		{
			"info_name": 100,
			"info_value": [0, "ADBC MockDriver Driver - Go"]
		},
		{
			"info_name": 101,
			"info_value": [0, "(unknown or development build)"]
		},
		{
			"info_name": 102,
			"info_value": [0, "(unknown or development build)"]
		},
		{
			"info_name": 103,
			"info_value": [2, 1001000]
		},
		{
			"info_name": 10001,
			"info_value": [0, "my custom info"]
		}
	]`})
	require.NoError(t, err)

	getInfoRecs := make([]arrow.Record, 0)
	for info.Next() {
		rec := info.Record()
		rec.Retain()
		defer rec.Release()
		getInfoRecs = append(getInfoRecs, rec)
	}
	getInfoTable := array.NewTableFromRecords(info.Schema(), getInfoRecs)
	defer getInfoTable.Release()

	require.Truef(t, array.TableEqual(expectedGetInfoTable, getInfoTable), "expected: %s\ngot: %s", expectedGetInfoTable, getInfoTable)

	// Can also access default Get/Set methods
	err = cnxn.(driverbase.Connection).SetOption(OptionKeyUnrecognized, "should-fail")
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unknown connection option 'unrecognized'", err.Error())

	stmt, err := cnxn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()
}

type driverImpl struct {
	driverbase.DriverImplBase
	mock *mock.Mock
}

// NewDatabase implements driverbase.DriverImpl.
func (drv *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return driverbase.NewDatabase(&databaseImpl{DatabaseImplBase: driverbase.NewDatabaseImplBase(&drv.DriverImplBase), drv: drv}), nil
}

type databaseImpl struct {
	driverbase.DatabaseImplBase
	drv *driverImpl
}

// Close implements adbc.Database.
func (db *databaseImpl) Close() error {
	return nil
}

// Open implements adbc.Database.
func (db *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	return &connectionImpl{ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase), db: db}, nil
}

// SetOptions implements adbc.Database.
func (d *databaseImpl) SetOptions(options map[string]string) error {
	for k, v := range options {
		if err := d.SetOption(k, v); err != nil {
			return err
		}
	}
	return nil
}

// Only need to implement keys we recognize.
// Any other values will fallthrough to default failure message.
func (d *databaseImpl) SetOption(key, value string) error {
	switch key {
	case OptionKeyRecognized:
		_ = value // pretend to recognize the setting
		return nil
	}
	return d.DatabaseImplBase.SetOption(key, value)
}

type connectionImpl struct {
	driverbase.ConnectionImplBase
	db *databaseImpl
}

// Close implements adbc.Connection.
func (cnxn *connectionImpl) Close() error {
	return nil
}

// GetObjects implements adbc.Connection.
func (cnxn *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	panic("unimplemented")
}

// GetTableSchema implements adbc.Connection.
func (cnxn *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	panic("unimplemented")
}

// GetTableTypes implements adbc.Connection.
func (cnxn *connectionImpl) GetTableTypes(context.Context) (array.RecordReader, error) {
	panic("unimplemented")
}

// NewStatement implements adbc.Connection.
func (cnxn *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{StatementImplBase: driverbase.NewStatementImplBase(&cnxn.ConnectionImplBase), cnxn: cnxn}, nil
}

// ReadPartition implements adbc.Connection.
func (cnxn *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	panic("unimplemented")
}

type statementImpl struct {
	driverbase.StatementImplBase
	cnxn *connectionImpl
}

// ExecuteSchema implements driverbase.StatementImpl.
func (*statementImpl) ExecuteSchema(context.Context) (*arrow.Schema, error) {
	panic("unimplemented")
}

// Bind implements adbc.Statement.
func (stmt *statementImpl) Bind(ctx context.Context, values arrow.Record) error {
	panic("unimplemented")
}

// BindStream implements adbc.Statement.
func (stmt *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	panic("unimplemented")
}

// Close implements adbc.Statement.
func (stmt *statementImpl) Close() error {
	return nil
}

// ExecutePartitions implements adbc.Statement.
func (stmt *statementImpl) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	panic("unimplemented")
}

// ExecuteQuery implements adbc.Statement.
func (stmt *statementImpl) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	panic("unimplemented")
}

// ExecuteUpdate implements adbc.Statement.
func (stmt *statementImpl) ExecuteUpdate(context.Context) (int64, error) {
	panic("unimplemented")
}

// GetParameterSchema implements adbc.Statement.
func (stmt *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	panic("unimplemented")
}

// Prepare implements adbc.Statement.
func (stmt *statementImpl) Prepare(context.Context) error {
	panic("unimplemented")
}

// SetOption implements adbc.Statement.
func (stmt *statementImpl) SetOption(key string, val string) error {
	panic("unimplemented")
}

// SetSqlQuery implements adbc.Statement.
func (stmt *statementImpl) SetSqlQuery(query string) error {
	panic("unimplemented")
}

// SetSubstraitPlan implements adbc.Statement.
func (stmt *statementImpl) SetSubstraitPlan(plan []byte) error {
	panic("unimplemented")
}
