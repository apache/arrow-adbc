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
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	OptionKeyRecognized   = "recognized"
	OptionKeyUnrecognized = "unrecognized"
)

// NewDriver creates a new adbc.Driver for testing. In addition to a memory.Allocator, it takes
// a slog.Handler to use for all structured logging as well as a useHelpers flag to determine whether
// the test should register helper methods or use the default driverbase implementation.
func NewDriver(alloc memory.Allocator, handler slog.Handler, useHelpers bool) adbc.Driver {
	info := driverbase.DefaultDriverInfo("MockDriver")
	_ = info.RegisterInfoCode(adbc.InfoCode(10_001), "my custom info")
	return driverbase.NewDriver(&driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc), handler: handler, useHelpers: useHelpers})
}

func TestDefaultDriver(t *testing.T) {
	var handler MockedHandler
	handler.On("Handle", mock.Anything, mock.Anything).Return(nil)

	ctx := context.TODO()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	drv := NewDriver(alloc, &handler, false) // Do not use helper implementations; only default behavior

	db, err := drv.NewDatabase(nil)
	require.NoError(t, err)
	defer validation.CheckedClose(t, db)

	require.NoError(t, db.SetOptions(map[string]string{OptionKeyRecognized: "should-pass"}))

	err = db.SetOptions(map[string]string{OptionKeyUnrecognized: "should-fail"})
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unknown database option 'unrecognized'", err.Error())

	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() {
		// Cannot close more than once
		require.NoError(t, cnxn.Close())
		require.Error(t, cnxn.Close())
	}()

	err = cnxn.Commit(ctx)
	require.Error(t, err)
	require.Equal(t, "Invalid State: [MockDriver] Cannot commit when autocommit is enabled", err.Error())

	err = cnxn.Rollback(ctx)
	require.Error(t, err)
	require.Equal(t, "Invalid State: [MockDriver] Cannot rollback when autocommit is enabled", err.Error())

	info, err := cnxn.GetInfo(ctx, nil)
	require.NoError(t, err)
	getInfoTable := tableFromRecordReader(info)
	defer getInfoTable.Release()

	// This is what the driverbase provided GetInfo result should look like out of the box,
	// with one custom setting registered at initialization
	expectedGetInfoTable, err := array.TableFromJSON(alloc, adbc.GetInfoSchema, []string{`[
		{
			"info_name": 0,
			"info_value": [0, "MockDriver"]
		},
		{
			"info_name": 1,
			"info_value": [0, "(unknown or development build)"]
		},
		{
			"info_name": 2,
			"info_value": [0, "(unknown or development build)"]
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
	defer expectedGetInfoTable.Release()

	require.Truef(t, array.TableEqual(expectedGetInfoTable, getInfoTable), "expected: %s\ngot: %s", expectedGetInfoTable, getInfoTable)

	_, err = cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] GetObjects", err.Error())

	_, err = cnxn.GetTableTypes(ctx)
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] GetTableTypes", err.Error())

	autocommit, err := cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyAutoCommit)
	require.NoError(t, err)
	require.Equal(t, adbc.OptionValueEnabled, autocommit)

	err = cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyAutoCommit, "false")
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unsupported connection option 'adbc.connection.autocommit'", err.Error())

	_, err = cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentCatalog)
	require.Error(t, err)
	require.Equal(t, "Not Found: [MockDriver] Unknown connection option 'adbc.connection.catalog'", err.Error())

	err = cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyCurrentCatalog, "test_catalog")
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unknown connection option 'adbc.connection.catalog'", err.Error())

	// We passed a mock handler into the driver to use for logs, so we can check actual messages logged
	expectedLogMessages := []logMessage{
		{Message: "Opening a new connection", Level: "INFO", Attrs: map[string]string{"withHelpers": "false"}},
	}

	logMessages := make([]logMessage, 0, len(handler.Calls))
	for _, call := range handler.Calls {
		sr, ok := call.Arguments.Get(1).(slog.Record)
		require.True(t, ok)
		logMessages = append(logMessages, newLogMessage(sr))
	}

	for _, expected := range expectedLogMessages {
		var found bool
		for _, message := range logMessages {
			if messagesEqual(message, expected) {
				found = true
				break
			}
		}
		require.Truef(t, found, "expected message was never logged: %v", expected)
	}

}

func TestCustomizedDriver(t *testing.T) {
	var handler MockedHandler
	handler.On("Handle", mock.Anything, mock.Anything).Return(nil)

	ctx := context.TODO()
	alloc := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer alloc.AssertSize(t, 0)

	drv := NewDriver(alloc, &handler, true) // Use helper implementations

	db, err := drv.NewDatabase(nil)
	require.NoError(t, err)
	defer validation.CheckedClose(t, db)

	require.NoError(t, db.SetOptions(map[string]string{OptionKeyRecognized: "should-pass"}))

	err = db.SetOptions(map[string]string{OptionKeyUnrecognized: "should-fail"})
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unknown database option 'unrecognized'", err.Error())

	cnxn, err := db.Open(ctx)
	require.NoError(t, err)
	defer validation.CheckedClose(t, cnxn)

	err = cnxn.Commit(ctx)
	require.Error(t, err)
	require.Equal(t, "Invalid State: [MockDriver] Cannot commit when autocommit is enabled", err.Error())

	err = cnxn.Rollback(ctx)
	require.Error(t, err)
	require.Equal(t, "Invalid State: [MockDriver] Cannot rollback when autocommit is enabled", err.Error())

	info, err := cnxn.GetInfo(ctx, nil)
	require.NoError(t, err)
	getInfoTable := tableFromRecordReader(info)
	defer getInfoTable.Release()

	// This is the arrow table representation of GetInfo produced by merging:
	//  - the default DriverInfo set at initialization
	//  - the DriverInfo set once in the NewDriver constructor
	//  - the DriverInfo set dynamically when GetInfo is called by implementing DriverInfoPreparer interface
	expectedGetInfoTable, err := array.TableFromJSON(alloc, adbc.GetInfoSchema, []string{`[
		{
			"info_name": 0,
			"info_value": [0, "MockDriver"]
		},
		{
			"info_name": 1,
			"info_value": [0, "(unknown or development build)"]
		},
		{
			"info_name": 2,
			"info_value": [0, "(unknown or development build)"]
		},
		{
			"info_name": 3,
			"info_value": [1, true]
		},
		{
			"info_name": 4,
			"info_value": [1, false]
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
		},
		{
			"info_name": 10002,
			"info_value": [0, "this was fetched dynamically"]
		}
	]`})
	require.NoError(t, err)
	defer expectedGetInfoTable.Release()

	require.Truef(t, array.TableEqual(expectedGetInfoTable, getInfoTable), "expected: %s\ngot: %s", expectedGetInfoTable, getInfoTable)

	dbObjects, err := cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	require.NoError(t, err)
	dbObjectsTable := tableFromRecordReader(dbObjects)
	defer dbObjectsTable.Release()

	// This is the arrow table representation of the GetObjects output we get by implementing
	// the simplified TableTypeLister interface
	expectedDbObjectsTable, err := array.TableFromJSON(alloc, adbc.GetObjectsSchema, []string{`[
		{
			"catalog_name": "default",
			"catalog_db_schemas": [
				{
					"db_schema_name": "public",
					"db_schema_tables": [
						{
							"table_name": "foo",
							"table_type": "TABLE",
							"table_columns": [
								{
									"column_name": "col1",
									"ordinal_position": 1,
									"remarks": "the first column"
								},
								{
									"column_name": "col2",
									"ordinal_position": 2,
									"remarks": "the second column"
								},
								{
									"column_name": "col3",
									"ordinal_position": 3,
									"remarks": "the third column"
								}
							]
						}
					]
				},
				{
					"db_schema_name": "test",
					"db_schema_tables": [
						{
							"table_name": "bar",
							"table_type": "TABLE"
						}
					]
				}
			]
		},
		{
			"catalog_name": "my_db",
			"catalog_db_schemas": [
				{
					"db_schema_name": "public",
					"db_schema_tables": [
						{
							"table_name": "baz",
							"table_type": "TABLE",
							"table_columns": [
								{
									"column_name": "col4",
									"ordinal_position": 1
								}
							],
							"table_constraints": [
								{
									"constraint_name": "baz_pk",
									"constraint_type": "PRIMARY KEY",
									"constraint_column_names": ["col4"]
								}
							]
						}
					]
				}
			]
		}
	]`})
	require.NoError(t, err)
	defer expectedDbObjectsTable.Release()

	require.Truef(t, array.TableEqual(expectedDbObjectsTable, dbObjectsTable), "expected: %s\ngot: %s", expectedDbObjectsTable, dbObjectsTable)

	tableTypes, err := cnxn.GetTableTypes(ctx)
	require.NoError(t, err)
	tableTypeTable := tableFromRecordReader(tableTypes)
	defer tableTypeTable.Release()

	// This is the arrow table representation of the GetTableTypes output we get by implementing
	// the simplified TableTypeLister interface
	expectedTableTypesTable, err := array.TableFromJSON(alloc, adbc.TableTypesSchema, []string{`[
		{ "table_type": "TABLE" },
		{ "table_type": "VIEW" }
	]`})
	require.NoError(t, err)
	defer expectedTableTypesTable.Release()

	require.Truef(t, array.TableEqual(expectedTableTypesTable, tableTypeTable), "expected: %s\ngot: %s", expectedTableTypesTable, tableTypeTable)

	autocommit, err := cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyAutoCommit)
	require.NoError(t, err)
	require.Equal(t, adbc.OptionValueEnabled, autocommit)

	// By implementing AutocommitSetter, we are able to successfully toggle autocommit
	err = cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyAutoCommit, "false")
	require.NoError(t, err)

	// We haven't implemented Commit, but we get NotImplemented instead of InvalidState because
	// Autocommit has been explicitly disabled
	err = cnxn.Commit(ctx)
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Commit", err.Error())

	// By implementing CurrentNamespacer, we can now get/set the current catalog/dbschema
	// Default current(catalog|dbSchema) is driver-specific, but the stub implementation falls back
	// to a 'not found' error instead of 'not implemented'
	_, err = cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentCatalog)
	require.Error(t, err)
	require.Equal(t, "Not Found: [MockDriver] failed to get current catalog: current catalog is not set", err.Error())

	err = cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyCurrentCatalog, "test_catalog")
	require.NoError(t, err)

	currentCatalog, err := cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentCatalog)
	require.NoError(t, err)
	require.Equal(t, "test_catalog", currentCatalog)

	_, err = cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentDbSchema)
	require.Error(t, err)
	require.Equal(t, "Not Found: [MockDriver] failed to get current db schema: current db schema is not set", err.Error())

	err = cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyCurrentDbSchema, "test_schema")
	require.NoError(t, err)

	currentDbSchema, err := cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentDbSchema)
	require.NoError(t, err)
	require.Equal(t, "test_schema", currentDbSchema)

	// We passed a mock handler into the driver to use for logs, so we can check actual messages logged
	expectedLogMessages := []logMessage{
		{Message: "Opening a new connection", Level: "INFO", Attrs: map[string]string{"withHelpers": "true"}},
		{Message: "SetAutocommit", Level: "DEBUG", Attrs: map[string]string{"enabled": "false"}},
		{Message: "SetCurrentCatalog", Level: "DEBUG", Attrs: map[string]string{"val": "test_catalog"}},
		{Message: "SetCurrentDbSchema", Level: "DEBUG", Attrs: map[string]string{"val": "test_schema"}},
	}

	logMessages := make([]logMessage, 0, len(handler.Calls))
	for _, call := range handler.Calls {
		sr, ok := call.Arguments.Get(1).(slog.Record)
		require.True(t, ok)
		logMessages = append(logMessages, newLogMessage(sr))
	}

	for _, expected := range expectedLogMessages {
		var found bool
		for _, message := range logMessages {
			if messagesEqual(message, expected) {
				found = true
				break
			}
		}
		require.Truef(t, found, "expected message was never logged: %v", expected)
	}
}

type driverImpl struct {
	driverbase.DriverImplBase

	handler    slog.Handler
	useHelpers bool
}

func (drv *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return drv.NewDatabaseWithContext(context.Background(), opts)
}

func (drv *driverImpl) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	dbBase, err := driverbase.NewDatabaseImplBase(ctx, &drv.DriverImplBase)
	if err != nil {
		return nil, err
	}
	db := driverbase.NewDatabase(
		&databaseImpl{
			DatabaseImplBase: dbBase,
			drv:              drv,
			useHelpers:       drv.useHelpers,
		})
	db.SetLogger(slog.New(drv.handler))
	return db, nil
}

type databaseImpl struct {
	driverbase.DatabaseImplBase
	drv *driverImpl

	useHelpers bool
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

func (db *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	db.Logger.Info("Opening a new connection", "withHelpers", db.useHelpers)
	cnxn := &connectionImpl{ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase), db: db}
	bldr := driverbase.NewConnectionBuilder(cnxn)
	if db.useHelpers { // this toggles between the NewDefaultDriver and NewCustomizedDriver scenarios
		return bldr.
			WithAutocommitSetter(cnxn).
			WithCurrentNamespacer(cnxn).
			WithTableTypeLister(cnxn).
			WithDriverInfoPreparer(cnxn).
			WithDbObjectsEnumerator(cnxn).
			WithConcurrency(1).
			Connection(), nil
	}
	return bldr.Connection(), nil
}

type connectionImpl struct {
	driverbase.ConnectionImplBase
	db *databaseImpl

	currentCatalog  string
	currentDbSchema string
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	stmt := &statement{
		StatementImplBase: driverbase.NewStatementImplBase(c.Base(), c.ErrorHelper),
		cnxn:              c,
	}
	return driverbase.NewStatement(stmt), nil
}

type statement struct {
	driverbase.StatementImplBase
	cnxn *connectionImpl
}

func (base *statement) Base() *driverbase.StatementImplBase {
	return &base.StatementImplBase
}

func (base *statement) Bind(ctx context.Context, values arrow.RecordBatch) error {
	return base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "Bind")
}

func (base *statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	return base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "BindStream")
}

func (base *statement) Close() error {
	return base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "Close")
}

func (base *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecutePartitions")
}

func (base *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	return nil, 0, base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteQuery")
}

func (base *statement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteSchema")
}

func (base *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return 0, base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteUpdate")
}

func (base *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetParameterSchema")
}

func (base *statement) Prepare(ctx context.Context) error {
	return base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "Prepare")
}

func (base *statement) SetSqlQuery(query string) error {
	return base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSqlQuery")
}

func (base statement) SetSubstraitPlan(plan []byte) error {
	return base.Base().ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan")
}

var dbObjects = map[string]map[string][]driverbase.TableInfo{
	"default": {
		"public": {
			{
				TableName: "foo",
				TableType: "TABLE",
				TableColumns: []driverbase.ColumnInfo{
					{
						ColumnName:      "col1",
						OrdinalPosition: driverbase.Nullable(int32(1)),
						Remarks:         driverbase.Nullable("the first column"),
					},
					{
						ColumnName:      "col2",
						OrdinalPosition: driverbase.Nullable(int32(2)),
						Remarks:         driverbase.Nullable("the second column"),
					},
					{
						ColumnName:      "col3",
						OrdinalPosition: driverbase.Nullable(int32(3)),
						Remarks:         driverbase.Nullable("the third column"),
					},
				},
			},
		},
		"test": {
			{TableName: "bar", TableType: "TABLE"},
		},
	},
	"my_db": {
		"public": {
			{
				TableName: "baz",
				TableType: "TABLE",
				TableColumns: []driverbase.ColumnInfo{
					{
						ColumnName:      "col4",
						OrdinalPosition: driverbase.Nullable(int32(1)),
					},
				},
				TableConstraints: []driverbase.ConstraintInfo{
					{
						ConstraintName:        driverbase.Nullable("baz_pk"),
						ConstraintType:        "PRIMARY KEY",
						ConstraintColumnNames: driverbase.RequiredList([]string{"col4"}),
					},
				},
			},
		},
	},
}

// GetCatalogs implements driverbase.DbObjectsEnumeratorV2.
func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	catalogs := make([]string, 0, len(dbObjects))
	for cat := range dbObjects {
		catalogs = append(catalogs, cat)
	}

	slices.Sort(catalogs)
	return catalogs, nil
}

// GetDBSchemasForCatalog implements driverbase.DbObjectsEnumeratorV2.
func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	schemas, ok := dbObjects[catalog]
	if !ok {
		return nil, fmt.Errorf("catalog %s not found", catalog)
	}

	dbSchemaNames := make([]string, 0, len(schemas))
	for sch := range schemas {
		dbSchemaNames = append(dbSchemaNames, sch)
	}

	slices.Sort(dbSchemaNames)
	return dbSchemaNames, nil
}

// GetTablesForDBSchema implements driverbase.DbObjectsEnumeratorV2.
func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	schemas, ok := dbObjects[catalog]
	if !ok {
		return nil, fmt.Errorf("catalog %s not found", catalog)
	}

	tables, ok := schemas[schema]
	if !ok {
		return nil, fmt.Errorf("dbSchema %s not found", schema)
	}

	slices.SortFunc(tables, func(a, b driverbase.TableInfo) int { return strings.Compare(a.TableName, b.TableName) })
	return tables, nil
}

func (c *connectionImpl) SetAutocommit(enabled bool) error {
	c.Base().Logger.Debug("SetAutocommit", "enabled", enabled)
	return nil
}

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	if c.currentCatalog == "" {
		return "", fmt.Errorf("current catalog is not set")
	}
	return c.currentCatalog, nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	if c.currentDbSchema == "" {
		return "", fmt.Errorf("current db schema is not set")
	}
	return c.currentDbSchema, nil
}

func (c *connectionImpl) SetCurrentCatalog(val string) error {
	c.Base().Logger.Debug("SetCurrentCatalog", "val", val)
	c.currentCatalog = val
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(val string) error {
	c.Base().Logger.Debug("SetCurrentDbSchema", "val", val)
	c.currentDbSchema = val
	return nil
}

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW"}, nil
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSql, true); err != nil {
		return err
	}
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSubstrait, false); err != nil {
		return err
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoCode(10_002), "this was fetched dynamically")
}

// MockedHandler is a mock.Mock that implements the slog.Handler interface.
// It is used to assert specific behavior for loggers it is injected into.
type MockedHandler struct {
	mock.Mock
}

func (h *MockedHandler) Enabled(ctx context.Context, level slog.Level) bool { return true }
func (h *MockedHandler) WithAttrs(attrs []slog.Attr) slog.Handler           { return h }
func (h *MockedHandler) WithGroup(name string) slog.Handler                 { return h }
func (h *MockedHandler) Handle(ctx context.Context, r slog.Record) error {
	// We only care to assert the message value, and want to isolate nondetermistic behavior (e.g. timestamp)
	args := h.Called(ctx, r)
	return args.Error(0)
}

// logMessage is a container for log attributes we would like to compare for equality during tests.
// It intentionally omits timestamps and other sources of nondeterminism.
type logMessage struct {
	Message string
	Level   string
	Attrs   map[string]string
}

// newLogMessage constructs a logMessage from a slog.Record, containing only deterministic fields.
func newLogMessage(r slog.Record) logMessage {
	message := logMessage{Message: r.Message, Level: r.Level.String(), Attrs: make(map[string]string)}
	r.Attrs(func(a slog.Attr) bool {
		message.Attrs[a.Key] = a.Value.String()
		return true
	})
	return message
}

// messagesEqual compares two logMessages and returns whether they are equal.
func messagesEqual(expected, actual logMessage) bool {
	if expected.Message != actual.Message {
		return false
	}
	if expected.Level != actual.Level {
		return false
	}
	if len(expected.Attrs) != len(actual.Attrs) {
		return false
	}
	for k, v := range expected.Attrs {
		if actual.Attrs[k] != v {
			return false
		}
	}
	return true
}

func tableFromRecordReader(rdr array.RecordReader) arrow.Table {
	defer rdr.Release()

	recs := make([]arrow.RecordBatch, 0)
	for rdr.Next() {
		rec := rdr.RecordBatch()
		rec.Retain()
		defer rec.Release()
		recs = append(recs, rec)
	}
	return array.NewTableFromRecords(rdr.Schema(), recs)
}

func TestRequiredList(t *testing.T) {
	v := driverbase.RequiredList([]string{"a", "b", "c"})
	result, err := json.Marshal(v)
	require.NoError(t, err)
	assert.JSONEq(t, `["a", "b", "c"]`, string(result))

	require.NoError(t, json.Unmarshal([]byte(`["d", "e", "f"]`), &v))
	assert.Equal(t, driverbase.RequiredList([]string{"d", "e", "f"}), v)
}
