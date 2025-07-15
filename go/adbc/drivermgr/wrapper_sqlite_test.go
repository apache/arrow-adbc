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
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	cnxn, err := dm.db.Open(dm.ctx)
	dm.NoError(err)
	defer validation.CheckedClose(dm.T(), cnxn)

	stmt, err := cnxn.NewStatement()
	dm.NoError(err)
	defer validation.CheckedClose(dm.T(), stmt)

	dm.NoError(stmt.SetSqlQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"))

	nrows, err := stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)
	dm.Equal(int64(0), nrows)

	dm.NoError(stmt.SetSqlQuery("INSERT INTO test_table (id, name) VALUES (1, 'test')"))

	nrows, err = stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)
	dm.Equal(int64(1), nrows)
}

func (dm *DriverMgrSuite) TearDownSuite() {
	dm.NoError(dm.db.Close())
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
					                "db_schema_name": "",
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
					                "db_schema_name": "",
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
					                "db_schema_name": "",
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
					                "db_schema_name": "",
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

func (dm *DriverMgrSuite) TestGetTableSchema() {
	schema, err := dm.conn.GetTableSchema(dm.ctx, nil, nil, "test_table")
	dm.NoError(err)

	expSchema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil)
	dm.True(expSchema.Equal(schema))
}

func (dm *DriverMgrSuite) TestGetTableSchemaInvalidTable() {
	_, err := dm.conn.GetTableSchema(dm.ctx, nil, nil, "unknown_table")
	dm.Error(err)
}

func (dm *DriverMgrSuite) TestGetTableSchemaCatalog() {
	catalog := "does_not_exist"
	schema, err := dm.conn.GetTableSchema(dm.ctx, &catalog, nil, "test_table")
	dm.Error(err)
	dm.Nil(schema)
}

func (dm *DriverMgrSuite) TestGetTableSchemaDBSchema() {
	dbSchema := "does_not_exist"
	schema, err := dm.conn.GetTableSchema(dm.ctx, nil, &dbSchema, "test_table")
	dm.Error(err)
	dm.Nil(schema)
}

func (dm *DriverMgrSuite) TestGetTableTypes() {
	rdr, err := dm.conn.GetTableTypes(dm.ctx)
	dm.NoError(err)
	defer rdr.Release()

	expSchema := adbc.TableTypesSchema
	dm.True(expSchema.Equal(rdr.Schema()))
	dm.True(rdr.Next())

	rec := rdr.Record()
	dm.Equal(int64(2), rec.NumRows())

	expTableTypes := []string{"table", "view"}
	dm.Contains(expTableTypes, rec.Column(0).ValueStr(0))
	dm.Contains(expTableTypes, rec.Column(0).ValueStr(1))
	dm.False(rdr.Next())
}

func (dm *DriverMgrSuite) TestCommit() {
	err := dm.conn.Commit(dm.ctx)
	dm.Error(err)
	dm.ErrorContains(err, "No active transaction, cannot commit")
}

func (dm *DriverMgrSuite) TestCommitAutocommitDisabled() {
	cnxnopt, ok := dm.conn.(adbc.PostInitOptions)
	dm.True(ok)

	dm.NoError(cnxnopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	dm.NoError(dm.conn.Commit(dm.ctx))
}

func (dm *DriverMgrSuite) TestRollback() {
	err := dm.conn.Rollback(dm.ctx)
	dm.Error(err)
	dm.ErrorContains(err, "No active transaction, cannot rollback")
}

func (dm *DriverMgrSuite) TestRollbackAutocommitDisabled() {
	cnxnopt, ok := dm.conn.(adbc.PostInitOptions)
	dm.True(ok)

	dm.NoError(cnxnopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
	dm.NoError(dm.conn.Rollback(dm.ctx))
}

func (dm *DriverMgrSuite) TestSqlExecute() {
	query := "SELECT 1"
	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	dm.Require().NoError(st.SetSqlQuery(query))
	defer validation.CheckedClose(dm.T(), st)

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
	defer validation.CheckedClose(dm.T(), st)

	dm.Require().NoError(st.SetSqlQuery(query))

	_, _, err = st.ExecuteQuery(dm.ctx)
	dm.Require().Error(err)

	var adbcErr adbc.Error
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
	defer validation.CheckedClose(dm.T(), st)

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
	defer validation.CheckedClose(dm.T(), st)

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

func (dm *DriverMgrSuite) TestGetParameterSchema() {
	query := "SELECT ?1, ?2"
	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	dm.Require().NoError(st.SetSqlQuery(query))
	defer validation.CheckedClose(dm.T(), st)
	dm.Require().NoError(st.Prepare(context.Background()))

	expSchema := arrow.NewSchema([]arrow.Field{
		{Name: "?1", Type: arrow.Null, Nullable: true},
		{Name: "?2", Type: arrow.Null, Nullable: true},
	}, nil)

	schema, err := st.GetParameterSchema()
	dm.NoError(err)

	dm.True(expSchema.Equal(schema))
}

func (dm *DriverMgrSuite) TestBindStream() {
	query := "SELECT ?1, ?2"
	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	dm.Require().NoError(st.SetSqlQuery(query))
	defer validation.CheckedClose(dm.T(), st)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "1", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "2", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"one", "two", "three"}, nil)

	rec1 := bldr.NewRecord()
	defer rec1.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{4, 5, 6}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"four", "five", "six"}, nil)

	rec2 := bldr.NewRecord()
	defer rec2.Release()

	recsIn := []arrow.Record{rec1, rec2}
	rdrIn, err := array.NewRecordReader(schema, recsIn)
	dm.NoError(err)

	dm.NoError(st.BindStream(dm.ctx, rdrIn))

	rdrOut, _, err := st.ExecuteQuery(dm.ctx)
	dm.NoError(err)
	defer rdrOut.Release()

	recsOut := make([]arrow.Record, 0)
	for rdrOut.Next() {
		rec := rdrOut.Record()
		rec.Retain()
		defer rec.Release()
		recsOut = append(recsOut, rec)
	}

	tableIn := array.NewTableFromRecords(schema, recsIn)
	defer tableIn.Release()
	tableOut := array.NewTableFromRecords(schema, recsOut)
	defer tableOut.Release()

	dm.Truef(array.TableEqual(tableIn, tableOut), "expected: %s\ngot: %s", tableIn, tableOut)
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
	require.NoError(t, db.Close())

	// set invalid entrypoint
	drv = drivermgr.Driver{}
	db, err = drv.NewDatabase(map[string]string{
		"driver":     "adbc_driver_sqlite",
		"entrypoint": "ThisSymbolDoesNotExist",
	})
	assert.Nil(t, db)
	var exp adbc.Error
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

func (dm *DriverMgrSuite) TestIngestStream() {
	// 1) Define the Arrow schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "col1", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "col2", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	// 2) Build two batches via JSON
	rec1, _, err := array.RecordFromJSON(
		memory.DefaultAllocator, schema,
		strings.NewReader(`[
            {"col1": 1, "col2": "one"},
            {"col1": 2, "col2": "two"},
            {"col1": 3, "col2": "three"}
        ]`),
	)
	dm.Require().NoError(err)
	defer rec1.Release()

	rec2, _, err := array.RecordFromJSON(
		memory.DefaultAllocator, schema,
		strings.NewReader(`[
            {"col1": 4, "col2": "four"},
            {"col1": 5, "col2": "five"}
        ]`),
	)
	dm.Require().NoError(err)
	defer rec2.Release()

	rdr, err := array.NewRecordReader(schema, []arrow.Record{rec1, rec2})
	dm.Require().NoError(err)
	defer rdr.Release()

	// 3) Use the IngestStream
	count, err := adbc.IngestStream(dm.ctx, dm.conn, rdr, "ingest_test", adbc.OptionValueIngestModeCreateAppend, adbc.IngestStreamOption{})
	dm.NoError(err)
	dm.Equal(int64(5), count, "should report 5 rows ingested")

	// 4) Verify with a simple COUNT(*) query
	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	defer validation.CheckedClose(dm.T(), st)

	dm.NoError(st.SetSqlQuery(`SELECT COUNT(*) AS cnt FROM ingest_test`))
	rdr2, _, err := st.ExecuteQuery(dm.ctx)
	dm.NoError(err)
	defer rdr2.Release()

	dm.True(rdr2.Next(), "expected one row with the count")
	recCount := rdr2.Record()
	cntArr := recCount.Column(0).(*array.Int64)
	dm.Equal(int64(5), cntArr.Value(0), "table should contain 5 rows")
	dm.False(rdr2.Next(), "no more rows expected")
}
