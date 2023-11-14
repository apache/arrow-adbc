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

//go:build cgo

package drivermgr_test

import (
	"context"
	"runtime"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DriverMgrSuite struct {
	suite.Suite

	ctx  context.Context
	drv  adbc.Driver
	db   adbc.Database
	conn adbc.Connection
}

func (dm *DriverMgrSuite) SetupSuite() {
	dm.ctx = context.TODO()
	dm.drv = drivermgr.Driver{}
	var err error
	dm.db, err = dm.drv.NewDatabase(map[string]string{
		"driver": "adbc_driver_sqlite",
	})
	dm.NoError(err)

	db, err := dm.db.Open(dm.ctx)
	dm.NoError(err)
	defer db.Close()

	stmt, err := db.NewStatement()
	dm.NoError(err)
	defer stmt.Close()

	err = stmt.SetSqlQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)")
	dm.NoError(err)

	nrows, err := stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)
	dm.Equal(int64(0), nrows)
}

func (dm *DriverMgrSuite) SetupTest() {
	cnxn, err := dm.db.Open(dm.ctx)
	dm.Require().NoError(err)
	dm.conn = cnxn
}

func (dm *DriverMgrSuite) TearDownTest() {
	dm.NoError(dm.conn.Close())
}

func (dm *DriverMgrSuite) TestMetadataGetInfo() {
	infoSchema := arrow.NewSchema([]arrow.Field{
		{Name: "info_name", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "info_value", Nullable: true, Type: arrow.DenseUnionOf([]arrow.Field{
			{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			{Name: "int64_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "int32_bitmask", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			{Name: "string_list", Type: arrow.ListOfField(arrow.Field{Name: "item", Type: arrow.BinaryTypes.String, Nullable: true}), Nullable: true},
			{Name: "int32_to_int32_list_map", Type: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.ListOf(arrow.PrimitiveTypes.Int32)), Nullable: true},
		}, []arrow.UnionTypeCode{0, 1, 2, 3, 4, 5})},
	}, nil)

	rdr, err := dm.conn.GetInfo(dm.ctx, nil)
	dm.NoError(err)
	dm.True(infoSchema.Equal(rdr.Schema()))
	rdr.Release()

	info := []adbc.InfoCode{
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoVendorName,
		adbc.InfoVendorVersion,
	}

	rdr, err = dm.conn.GetInfo(dm.ctx, info)
	dm.NoError(err)
	sc := rdr.Schema()
	dm.True(infoSchema.Equal(sc))
	rdr.Release()

	// TODO(apache/arrow-nanoarrow#76): values are not checked because go fails to import the union values
}

func (dm *DriverMgrSuite) TestGetObjects() {
	rdr, err := dm.conn.GetObjects(dm.ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := adbc.GetObjectsSchema
	dm.True(expSchema.Equal(rdr.Schema()))
	dm.True(rdr.Next())

	rec := rdr.Record()
	dm.Equal(int64(1), rec.NumRows())
	expRec, _, err := array.RecordFromJSON(
		memory.DefaultAllocator,
		expSchema,
		strings.NewReader(
			`[
				{
					"catalog_name": "main",
					"catalog_db_schemas": [
						{
							"db_schema_tables": [
								{
									"table_name": "test_table",
									"table_type": "table",
									"table_columns": [
										{
											"column_name": "id",
											"ordinal_position": 1,
											"xdbc_type_name": "INTEGER",
											"xdbc_nullable": 1,
											"xdbc_is_nullable": "YES"
										},
										{
											"column_name": "name",
											"ordinal_position": 2,
											"xdbc_type_name": "TEXT",
											"xdbc_nullable": 1,
											"xdbc_is_nullable": "YES"
										}
									],
									"table_constraints": [
										{
											"constraint_type": "PRIMARY KEY",
											"constraint_column_names": ["id"]
										}
									]
								}
							]
						}
					]
				}
			]`))
	dm.NoError(err)
	defer expRec.Release()

	dm.Truef(array.RecordEqual(expRec, rec), "expected: %s\ngot: %s", expRec, rec)
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestGetObjectsCatalog() {
	catalog := "does_not_exist"
	rdr, err := dm.conn.GetObjects(dm.ctx, adbc.ObjectDepthAll, &catalog, nil, nil, nil, nil)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := adbc.GetObjectsSchema
	dm.True(expSchema.Equal(rdr.Schema()))
	dm.True(rdr.Next())

	rec := rdr.Record()
	dm.Equal(int64(0), rec.NumRows())
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestGetObjectsDBSchema() {
	dbSchema := "does_not_exist"
	rdr, err := dm.conn.GetObjects(dm.ctx, adbc.ObjectDepthAll, nil, &dbSchema, nil, nil, nil)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := adbc.GetObjectsSchema
	dm.True(expSchema.Equal(rdr.Schema()))
	dm.True(rdr.Next())

	rec := rdr.Record()
	dm.Equal(int64(1), rec.NumRows())
	expRec, _, err := array.RecordFromJSON(
		memory.DefaultAllocator,
		expSchema,
		strings.NewReader(
			`[
				{
					"catalog_name": "main",
					"catalog_db_schemas": []
				}
			]`))
	dm.NoError(err)
	defer expRec.Release()

	dm.Truef(array.RecordEqual(expRec, rec), "expected: %s\ngot: %s", expRec, rec)
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestGetObjectsTableName() {
	tableName := "does_not_exist"
	rdr, err := dm.conn.GetObjects(dm.ctx, adbc.ObjectDepthAll, nil, nil, &tableName, nil, nil)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := adbc.GetObjectsSchema
	dm.True(expSchema.Equal(rdr.Schema()))
	dm.True(rdr.Next())

	rec := rdr.Record()
	dm.Equal(int64(1), rec.NumRows())
	expRec, _, err := array.RecordFromJSON(
		memory.DefaultAllocator,
		expSchema,
		strings.NewReader(
			`[
				{
					"catalog_name": "main",
					"catalog_db_schemas": [
						{
							"db_schema_tables": []
						}
					]
				}
			]`))
	dm.NoError(err)
	defer expRec.Release()

	dm.Truef(array.RecordEqual(expRec, rec), "expected: %s\ngot: %s", expRec, rec)
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestGetObjectsColumnName() {
	columnName := "name"
	rdr, err := dm.conn.GetObjects(dm.ctx, adbc.ObjectDepthAll, nil, nil, nil, &columnName, nil)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := adbc.GetObjectsSchema
	dm.True(expSchema.Equal(rdr.Schema()))
	dm.True(rdr.Next())

	rec := rdr.Record()
	dm.Equal(int64(1), rec.NumRows())
	expRec, _, err := array.RecordFromJSON(
		memory.DefaultAllocator,
		expSchema,
		strings.NewReader(
			`[
				{
					"catalog_name": "main",
					"catalog_db_schemas": [
						{
							"db_schema_tables": [
								{
									"table_name": "test_table",
									"table_type": "table",
									"table_columns": [
										{
											"column_name": "name",
											"ordinal_position": 2,
											"xdbc_type_name": "TEXT",
											"xdbc_nullable": 1,
											"xdbc_is_nullable": "YES"
										}
									],
									"table_constraints": [
										{
											"constraint_type": "PRIMARY KEY",
											"constraint_column_names": ["id"]
										}
									]
								}
							]
						}
					]
				}
			]`))
	dm.NoError(err)
	defer expRec.Release()

	dm.Truef(array.RecordEqual(expRec, rec), "expected: %s\ngot: %s", expRec, rec)
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestGetObjectsTableType() {
	rdr, err := dm.conn.GetObjects(dm.ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, []string{"not_a_table"})
	dm.NoError(err)
	defer rdr.Release()

	expSchema := adbc.GetObjectsSchema
	dm.True(expSchema.Equal(rdr.Schema()))
	dm.True(rdr.Next())

	rec := rdr.Record()
	dm.Equal(int64(1), rec.NumRows())
	expRec, _, err := array.RecordFromJSON(
		memory.DefaultAllocator,
		expSchema,
		strings.NewReader(
			`[
				{
					"catalog_name": "main",
					"catalog_db_schemas": [
						{
							"db_schema_tables": []
						}
					]
				}
			]`))
	dm.NoError(err)
	defer expRec.Release()

	dm.Truef(array.RecordEqual(expRec, rec), "expected: %s\ngot: %s", expRec, rec)
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestSqlExecute() {
	query := "SELECT 1"
	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	dm.Require().NoError(st.SetSqlQuery(query))
	defer st.Close()

	rdr, _, err := st.ExecuteQuery(dm.ctx)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := arrow.NewSchema([]arrow.Field{{Name: "1", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	dm.True(expSchema.Equal(rdr.Schema()), expSchema.String(), rdr.Schema().String())
	expRec, _, err := array.RecordFromJSON(memory.DefaultAllocator, expSchema, strings.NewReader(`[{"1": 1}]`))
	dm.Require().NoError(err)
	defer expRec.Release()

	dm.True(rdr.Next())
	dm.Truef(array.RecordEqual(expRec, rdr.Record()), "expected: %s\ngot: %s", expRec, rdr.Record())
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestSqlExecuteInvalid() {
	query := "INVALID"
	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	defer st.Close()

	dm.Require().NoError(st.SetSqlQuery(query))

	_, _, err = st.ExecuteQuery(dm.ctx)
	dm.Require().Error(err)

	var adbcErr *adbc.Error
	dm.ErrorAs(err, &adbcErr)
	dm.ErrorContains(adbcErr, "[SQLite] Failed to prepare query:")
	dm.ErrorContains(adbcErr, "syntax error")
	dm.Equal(adbc.StatusInvalidArgument, adbcErr.Code)
}

func (dm *DriverMgrSuite) TestSqlPrepare() {
	query := "SELECT 1"
	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	dm.Require().NoError(st.SetSqlQuery(query))
	defer st.Close()

	dm.Require().NoError(st.Prepare(dm.ctx))
	rdr, _, err := st.ExecuteQuery(dm.ctx)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := arrow.NewSchema([]arrow.Field{{Name: "1", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	dm.True(expSchema.Equal(rdr.Schema()), expSchema.String(), rdr.Schema().String())
	expRec, _, err := array.RecordFromJSON(memory.DefaultAllocator, expSchema, strings.NewReader(`[{"1": 1}]`))
	dm.Require().NoError(err)
	defer expRec.Release()

	dm.True(rdr.Next())
	dm.Truef(array.RecordEqual(expRec, rdr.Record()), "expected: %s\ngot: %s", expRec, rdr.Record())
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestSqlPrepareMultipleParams() {
	paramSchema := arrow.NewSchema([]arrow.Field{
		{Name: "1", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "2", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	// go arrow doesn't yet support duplicately named fields so
	// let's use sqlite syntax to name the fields
	query := "SELECT ?1, ?2"

	params, _, err := array.RecordFromJSON(memory.DefaultAllocator, paramSchema,
		strings.NewReader(`[{"1": 1, "2": "foo"}, {"1": 2, "2": "bar"}]`))
	dm.Require().NoError(err)
	defer params.Release()

	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	dm.Require().NoError(st.SetSqlQuery(query))
	defer st.Close()

	dm.NoError(st.Prepare(dm.ctx))
	dm.NoError(st.Bind(dm.ctx, params))

	rdr, _, err := st.ExecuteQuery(dm.ctx)
	dm.NoError(err)
	defer rdr.Release()

	dm.True(rdr.Next())
	rec := rdr.Record()
	dm.Truef(array.RecordEqual(params, rec), "expected: %s\ngot: %s", params, rec)
	dm.False(rdr.Next())
}

func TestDriverMgr(t *testing.T) {
	suite.Run(t, new(DriverMgrSuite))
}

func TestDriverMgrCustomInitFunc(t *testing.T) {
	// explicitly set entrypoint
	var drv drivermgr.Driver
	db, err := drv.NewDatabase(map[string]string{
		"driver":     "adbc_driver_sqlite",
		"entrypoint": "AdbcDriverInit",
	})
	assert.NoError(t, err)
	cnxn, err := db.Open(context.Background())
	assert.NoError(t, err)
	require.NoError(t, cnxn.Close())

	// set invalid entrypoint
	drv = drivermgr.Driver{}
	db, err = drv.NewDatabase(map[string]string{
		"driver":     "adbc_driver_sqlite",
		"entrypoint": "ThisSymbolDoesNotExist",
	})
	assert.Nil(t, db)
	var exp *adbc.Error
	assert.ErrorAs(t, err, &exp)
	assert.Equal(t, adbc.StatusInternal, exp.Code)
	if runtime.GOOS == "windows" {
		assert.Contains(t, exp.Msg, "GetProcAddress(ThisSymbolDoesNotExist) failed")
	} else {
		assert.Contains(t, exp.Msg, "dlsym(ThisSymbolDoesNotExist) failed")
	}
	switch runtime.GOOS {
	case "darwin":
		assert.Contains(t, exp.Msg, "ThisSymbolDoesNotExist): symbol not found")
	case "windows":
	default:
		assert.Contains(t, exp.Msg, "undefined symbol: ThisSymbolDoesNotExist")
	}
}
