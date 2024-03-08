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
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
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

func NewDriver(alloc memory.Allocator, useHelpers bool) adbc.Driver {
	info := driverbase.DefaultDriverInfo("MockDriver")
	_ = info.RegisterInfoCode(adbc.InfoCode(10_001), "my custom info")
	return driverbase.NewDriver(&driverImpl{DriverImplBase: driverbase.NewDriverImplBase(info, alloc), useHelpers: useHelpers})
}

func TestDefaultDriver(t *testing.T) {
	ctx := context.TODO()
	alloc := memory.DefaultAllocator
	drv := NewDriver(alloc, false)

	db, err := drv.NewDatabase(nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.SetOptions(map[string]string{OptionKeyRecognized: "should-pass"}))

	err = db.SetOptions(map[string]string{OptionKeyUnrecognized: "should-fail"})
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unknown database option 'unrecognized'", err.Error())

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

	getInfoTable := tableFromRecordReader(info)
	defer getInfoTable.Release()

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
}

func TestCustomizedDriver(t *testing.T) {
	ctx := context.TODO()
	alloc := memory.DefaultAllocator
	drv := NewDriver(alloc, true)

	db, err := drv.NewDatabase(nil)
	require.NoError(t, err)
	defer db.Close()

	require.NoError(t, db.SetOptions(map[string]string{OptionKeyRecognized: "should-pass"}))

	err = db.SetOptions(map[string]string{OptionKeyUnrecognized: "should-fail"})
	require.Error(t, err)
	require.Equal(t, "Not Implemented: [MockDriver] Unknown database option 'unrecognized'", err.Error())

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

	getInfoTable := tableFromRecordReader(info)
	defer getInfoTable.Release()

	require.Truef(t, array.TableEqual(expectedGetInfoTable, getInfoTable), "expected: %s\ngot: %s", expectedGetInfoTable, getInfoTable)

	dbObjects, err := cnxn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	require.NoError(t, err)

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
							"table_columns": [],
							"table_constraints": []
						}
					]
				},
				{
					"db_schema_name": "test",
					"db_schema_tables": [
						{
							"table_name": "bar",
							"table_type": "TABLE",
							"table_columns": [],
							"table_constraints": []
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
							"table_columns": [],
							"table_constraints": []
						}
					]
				}
			]
		}
	]`})
	require.NoError(t, err)

	dbObjectsTable := tableFromRecordReader(dbObjects)
	defer dbObjectsTable.Release()

	require.Truef(t, array.TableEqual(expectedDbObjectsTable, dbObjectsTable), "expected: %s\ngot: %s", expectedDbObjectsTable, dbObjectsTable)

	tableTypes, err := cnxn.GetTableTypes(ctx)
	require.NoError(t, err)

	// This is the arrow table representation of the GetTableTypes output we get by implementing
	// the simplified TableTypeLister interface
	expectedTableTypesTable, err := array.TableFromJSON(alloc, adbc.TableTypesSchema, []string{`[
		{ "table_type": "TABLE" },
		{ "table_type": "VIEW" }
	]`})
	require.NoError(t, err)

	tableTypeTable := tableFromRecordReader(tableTypes)
	defer tableTypeTable.Release()

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
	// Default current(catalog|dSchema) is driver-specific, but the stub implementation falls back
	// to a 'not found' error instead of 'not implemented'
	_, err = cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentCatalog)
	require.Error(t, err)
	require.Equal(t, "Not Found: [MockDriver] The current catalog has not been set", err.Error())

	err = cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyCurrentCatalog, "test_catalog")
	require.NoError(t, err)

	currentCatalog, err := cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentCatalog)
	require.NoError(t, err)
	require.Equal(t, "test_catalog", currentCatalog)

	_, err = cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentDbSchema)
	require.Error(t, err)
	require.Equal(t, "Not Found: [MockDriver] The current db schema has not been set", err.Error())

	err = cnxn.(adbc.GetSetOptions).SetOption(adbc.OptionKeyCurrentDbSchema, "test_schema")
	require.NoError(t, err)

	currentDbSchema, err := cnxn.(adbc.GetSetOptions).GetOption(adbc.OptionKeyCurrentDbSchema)
	require.NoError(t, err)
	require.Equal(t, "test_schema", currentDbSchema)
}

type driverImpl struct {
	driverbase.DriverImplBase

	useHelpers bool
}

func (drv *driverImpl) NewDatabase(opts map[string]string) (adbc.Database, error) {
	var handler MockedHandler
	db := driverbase.NewDatabase(
		&databaseImpl{DatabaseImplBase: driverbase.NewDatabaseImplBase(&drv.DriverImplBase),
			drv:        drv,
			useHelpers: drv.useHelpers,
		})
	db.SetLogger(slog.New(&handler))
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
	cnxn := &connectionImpl{ConnectionImplBase: driverbase.NewConnectionImplBase(&db.DatabaseImplBase), db: db}
	bldr := driverbase.NewConnectionBuilder(cnxn)
	if db.useHelpers { // this toggles between the NewDefaultDriver and NewCustomizedDriver scenarios
		return bldr.
			WithAutocommitSetter(cnxn).
			WithCurrentNamespacer(cnxn).
			WithTableTypeLister(cnxn).
			WithDriverInfoPreparer(cnxn).
			WithDbObjectsEnumerator(cnxn).
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

func (c *connectionImpl) SetAutocommit(enabled bool) error {
	return nil
}

func (c *connectionImpl) GetCurrentCatalog() (string, bool) {
	if c.currentCatalog == "" {
		return "", false
	}
	return c.currentCatalog, true
}

func (c *connectionImpl) GetCurrentDbSchema() (string, bool) {
	if c.currentDbSchema == "" {
		return "", false
	}
	return c.currentDbSchema, true
}

func (c *connectionImpl) SetCurrentCatalog(val string) error {
	c.currentCatalog = val
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(val string) error {
	c.currentDbSchema = val
	return nil
}

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW"}, nil
}

func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	return c.ConnectionImplBase.DriverInfo.RegisterInfoCode(adbc.InfoCode(10_002), "this was fetched dynamically")
}

func (c *connectionImpl) GetObjectsCatalogs(ctx context.Context, catalog *string) ([]string, error) {
	return []string{"default", "my_db"}, nil
}

func (c *connectionImpl) GetObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, metadataRecords []internal.Metadata) (map[string][]string, error) {
	return map[string][]string{
		"default": {"public", "test"},
		"my_db":   {"public"},
	}, nil
}

func (c *connectionImpl) GetObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (map[internal.CatalogAndSchema][]internal.TableInfo, error) {
	return map[internal.CatalogAndSchema][]internal.TableInfo{
		{Catalog: "default", Schema: "public"}: {internal.TableInfo{Name: "foo", TableType: "TABLE"}},
		{Catalog: "default", Schema: "test"}:   {internal.TableInfo{Name: "bar", TableType: "TABLE"}},
		{Catalog: "my_db", Schema: "public"}:   {internal.TableInfo{Name: "baz", TableType: "TABLE"}},
	}, nil
}

func tableFromRecordReader(rdr array.RecordReader) arrow.Table {
	recs := make([]arrow.Record, 0)
	for rdr.Next() {
		rec := rdr.Record()
		rec.Retain()
		defer rec.Release()
		recs = append(recs, rec)
	}
	return array.NewTableFromRecords(rdr.Schema(), recs)
}
