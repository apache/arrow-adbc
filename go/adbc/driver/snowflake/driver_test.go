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

package snowflake_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/decimal128"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/google/uuid"
	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SnowflakeQuirks struct {
	dsn         string
	mem         *memory.CheckedAllocator
	connector   gosnowflake.Connector
	catalogName string
	schemaName  string
}

func (s *SnowflakeQuirks) SetupDriver(t *testing.T) adbc.Driver {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	cfg, err := gosnowflake.ParseDSN(s.dsn)
	require.NoError(t, err)

	cfg.Schema = s.schemaName
	s.connector = gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cfg)
	return driver.NewDriver(s.mem)
}

func (s *SnowflakeQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	s.mem.AssertSize(t, 0)
}

func (s *SnowflakeQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI:   s.dsn,
		driver.OptionSchema: s.schemaName,
		// use int64 not decimal128 for the tests
		driver.OptionUseHighPrecision: adbc.OptionValueDisabled,
	}
}

func (s *SnowflakeQuirks) getSqlTypeFromArrowType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "INTEGER"
	case arrow.FLOAT32:
		return "float4"
	case arrow.FLOAT64:
		return "double"
	case arrow.STRING:
		return "text"
	default:
		return ""
	}
}

func getArr(arr arrow.Array) interface{} {
	switch arr := arr.(type) {
	case *array.Int8:
		v := arr.Int8Values()
		return gosnowflake.Array(&v)
	case *array.Uint8:
		v := arr.Uint8Values()
		return gosnowflake.Array(&v)
	case *array.Int16:
		v := arr.Int16Values()
		return gosnowflake.Array(&v)
	case *array.Uint16:
		v := arr.Uint16Values()
		return gosnowflake.Array(&v)
	case *array.Int32:
		v := arr.Int32Values()
		return gosnowflake.Array(&v)
	case *array.Uint32:
		v := arr.Uint32Values()
		return gosnowflake.Array(&v)
	case *array.Int64:
		v := arr.Int64Values()
		return gosnowflake.Array(&v)
	case *array.Uint64:
		v := arr.Uint64Values()
		return gosnowflake.Array(&v)
	case *array.Float32:
		v := arr.Float32Values()
		return gosnowflake.Array(&v)
	case *array.Float64:
		v := arr.Float64Values()
		return gosnowflake.Array(&v)
	case *array.String:
		v := make([]string, arr.Len())
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				continue
			}
			v[i] = arr.Value(i)
		}
		return gosnowflake.Array(&v)
	default:
		panic(fmt.Errorf("unimplemented type %s", arr.DataType()))
	}
}

func (s *SnowflakeQuirks) CreateSampleTable(tableName string, r arrow.Record) error {
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE TABLE ")
	b.WriteString(tableName)
	b.WriteString(" (")

	for i := 0; i < int(r.NumCols()); i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		f := r.Schema().Field(i)
		b.WriteString(f.Name)
		b.WriteByte(' ')
		b.WriteString(s.getSqlTypeFromArrowType(f.Type))
	}

	b.WriteString(")")
	db := sql.OpenDB(s.connector)
	defer db.Close()

	if _, err := db.Exec(b.String()); err != nil {
		return err
	}

	insertQuery := "INSERT INTO " + tableName + " VALUES ("
	bindings := strings.Repeat("?,", int(r.NumCols()))
	insertQuery += bindings[:len(bindings)-1] + ")"

	args := make([]interface{}, 0, r.NumCols())
	for _, col := range r.Columns() {
		args = append(args, getArr(col))
	}

	_, err := db.Exec(insertQuery, args...)
	return err
}

func (s *SnowflakeQuirks) DropTable(cnxn adbc.Connection, tblname string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err = stmt.SetSqlQuery(`DROP TABLE IF EXISTS ` + tblname); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (s *SnowflakeQuirks) Alloc() memory.Allocator               { return s.mem }
func (s *SnowflakeQuirks) BindParameter(_ int) string            { return "?" }
func (s *SnowflakeQuirks) SupportsBulkIngest(string) bool        { return true }
func (s *SnowflakeQuirks) SupportsConcurrentStatements() bool    { return true }
func (s *SnowflakeQuirks) SupportsCurrentCatalogSchema() bool    { return true }
func (s *SnowflakeQuirks) SupportsExecuteSchema() bool           { return true }
func (s *SnowflakeQuirks) SupportsGetSetOptions() bool           { return true }
func (s *SnowflakeQuirks) SupportsPartitionedData() bool         { return false }
func (s *SnowflakeQuirks) SupportsStatistics() bool              { return false }
func (s *SnowflakeQuirks) SupportsTransactions() bool            { return true }
func (s *SnowflakeQuirks) SupportsGetParameterSchema() bool      { return false }
func (s *SnowflakeQuirks) SupportsDynamicParameterBinding() bool { return false }
func (s *SnowflakeQuirks) Catalog() string                       { return s.catalogName }
func (s *SnowflakeQuirks) DBSchema() string                      { return s.schemaName }
func (s *SnowflakeQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Snowflake Driver - Go"
	// runtime/debug.ReadBuildInfo doesn't currently work for tests
	// github.com/golang/go/issues/33976
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverADBCVersion:
		return adbc.AdbcVersion1_1_0
	case adbc.InfoVendorName:
		return "Snowflake"
	}

	return nil
}

func (s *SnowflakeQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	switch dt.ID() {
	case arrow.STRING:
		return arrow.MetadataFrom(map[string]string{
			"DATA_TYPE": "VARCHAR(16777216)", "PRIMARY_KEY": "N",
		})
	case arrow.INT64:
		return arrow.MetadataFrom(map[string]string{
			"DATA_TYPE": "NUMBER(38,0)", "PRIMARY_KEY": "N",
		})
	}
	return arrow.Metadata{}
}

func createTempSchema(database string, uri string) string {
	db, err := sql.Open("snowflake", uri)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	schemaName := strings.ToUpper("ADBC_TESTING_" + strings.ReplaceAll(uuid.New().String(), "-", "_"))
	_, err = db.Exec(`CREATE SCHEMA ` + database + `.` + schemaName)
	if err != nil {
		panic(err)
	}

	return schemaName
}

func dropTempSchema(uri, schema string) {
	db, err := sql.Open("snowflake", uri)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	_, err = db.Exec(`DROP SCHEMA ` + schema)
	if err != nil {
		panic(err)
	}
}

func withQuirks(t *testing.T, fn func(*SnowflakeQuirks)) {
	uri := os.Getenv("SNOWFLAKE_URI")
	database := os.Getenv("SNOWFLAKE_DATABASE")

	if uri == "" {
		t.Skip("no SNOWFLAKE_URI defined, skip snowflake driver tests")
	} else if database == "" {
		t.Skip("no SNOWFLAKE_DATABASE defined, skip snowflake driver tests")
	}

	// avoid multiple runs clashing by operating in a fresh schema and then
	// dropping that schema when we're done.
	q := &SnowflakeQuirks{dsn: uri, catalogName: database, schemaName: createTempSchema(database, uri)}
	defer dropTempSchema(uri, q.schemaName)

	fn(q)
}

func TestValidation(t *testing.T) {
	withQuirks(t, func(q *SnowflakeQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

func TestSnowflake(t *testing.T) {
	withQuirks(t, func(q *SnowflakeQuirks) {
		suite.Run(t, &SnowflakeTests{Quirks: q})
	})
}

// ---- Additional Tests --------------------

type SnowflakeTests struct {
	suite.Suite

	Quirks *SnowflakeQuirks

	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (suite *SnowflakeTests) SetupSuite() {
	var err error
	suite.ctx = context.Background()
	suite.driver = suite.Quirks.SetupDriver(suite.T())
	suite.db, err = suite.driver.NewDatabase(suite.Quirks.DatabaseOptions())
	suite.NoError(err)
}

func (suite *SnowflakeTests) SetupTest() {
	var err error
	suite.cnxn, err = suite.db.Open(suite.ctx)
	suite.NoError(err)

	suite.stmt, err = suite.cnxn.NewStatement()
	suite.NoError(err)
}

func (suite *SnowflakeTests) TearDownTest() {
	suite.NoError(suite.stmt.Close())
	suite.NoError(suite.cnxn.Close())
}

func (suite *SnowflakeTests) TearDownSuite() {
	suite.NoError(suite.db.Close())
	suite.db = nil
}

func (suite *SnowflakeTests) TestSqlIngestTimestamp() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest"))

	sc := arrow.NewSchema([]arrow.Field{{
		Name: "col", Type: arrow.FixedWidthTypes.Timestamp_us,
		Nullable: true,
	}, {
		Name: "col2", Type: arrow.FixedWidthTypes.Time64us,
		Nullable: true,
	}, {
		Name: "col3", Type: arrow.PrimitiveTypes.Int64,
		Nullable: true,
	},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()

	tbldr := bldr.Field(0).(*array.TimestampBuilder)
	tbldr.AppendValues([]arrow.Timestamp{0, 0, 42}, []bool{false, true, true})
	tmbldr := bldr.Field(1).(*array.Time64Builder)
	tmbldr.AppendValues([]arrow.Time64{420000, 0, 86000}, []bool{true, false, true})
	ibldr := bldr.Field(2).(*array.Int64Builder)
	ibldr.AppendValues([]int64{-1, 25, 0}, []bool{true, true, false})

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT * FROM bulk_ingest ORDER BY \"col\" ASC NULLS FIRST"))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()
	suite.Truef(array.RecordEqual(rec, result), "expected: %s\ngot: %s", rec, result)
	suite.False(rdr.Next())

	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestStatementEmptyResultSet() {
	// Regression test for https://github.com/apache/arrow-adbc/issues/863
	suite.NoError(suite.stmt.SetSqlQuery("SHOW WAREHOUSES"))

	// XXX: there IS data in this result set, but Snowflake doesn't
	// appear to support getting the results as Arrow
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.True(rdr.Next())
	rec := rdr.Record()
	suite.Equal(n, rec.NumRows())
	suite.EqualValues(26, rec.NumCols())

	suite.False(rdr.Next())
	suite.NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestMetadataGetObjectsColumnsXdbc() {

	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest"))

	mdInts := make(map[string]string)
	mdInts["TYPE_NAME"] = "NUMERIC"
	mdInts["ORDINAL_POSITION"] = "1"
	mdInts["XDBC_DATA_TYPE"] = strconv.Itoa(int(arrow.PrimitiveTypes.Int64.ID()))
	mdInts["XDBC_TYPE_NAME"] = "NUMERIC"
	mdInts["XDBC_SQL_DATA_TYPE"] = strconv.Itoa(int(internal.XdbcDataType_XDBC_BIGINT))
	mdInts["XDBC_NULLABLE"] = strconv.FormatBool(true)
	mdInts["XDBC_IS_NULLABLE"] = "YES"
	mdInts["XDBC_PRECISION"] = strconv.Itoa(38)
	mdInts["XDBC_SCALE"] = strconv.Itoa(0)
	mdInts["XDBC_NUM_PREC_RADIX"] = strconv.Itoa(10)

	mdStrings := make(map[string]string)
	mdStrings["TYPE_NAME"] = "TEXT"
	mdStrings["ORDINAL_POSITION"] = "2"
	mdStrings["XDBC_DATA_TYPE"] = strconv.Itoa(int(arrow.BinaryTypes.String.ID()))
	mdStrings["XDBC_TYPE_NAME"] = "TEXT"
	mdStrings["XDBC_SQL_DATA_TYPE"] = strconv.Itoa(int(internal.XdbcDataType_XDBC_VARCHAR))
	mdStrings["XDBC_IS_NULLABLE"] = "YES"
	mdStrings["CHARACTER_MAXIMUM_LENGTH"] = strconv.Itoa(16777216)
	mdStrings["XDBC_CHAR_OCTET_LENGTH"] = strconv.Itoa(16777216)

	rec, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true, Metadata: arrow.MetadataFrom(mdInts)},
			{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true, Metadata: arrow.MetadataFrom(mdStrings)},
		}, nil), strings.NewReader(`[
			{"int64s": 42, "strings": "foo"},
			{"int64s": -42, "strings": null},
			{"int64s": null, "strings": ""}
		]`))
	suite.Require().NoError(err)
	defer rec.Release()

	suite.Require().NoError(suite.Quirks.CreateSampleTable("bulk_ingest", rec))

	tests := []struct {
		name             string
		colnames         []string
		positions        []string
		dataTypes        []string
		comments         []string
		xdbcDataType     []string
		xdbcTypeName     []string
		xdbcSqlDataType  []string
		xdbcNullable     []string
		xdbcIsNullable   []string
		xdbcScale        []string
		xdbcNumPrecRadix []string
		xdbcCharMaxLen   []string
		xdbcCharOctetLen []string
		xdbcDateTimeSub  []string
	}{
		{
			"BASIC",                       // name
			[]string{"int64s", "strings"}, // colNames
			[]string{"1", "2"},            // positions
			[]string{"NUMBER", "TEXT"},    // dataTypes
			[]string{"", ""},              // comments
			[]string{"9", "13"},           // xdbcDataType
			[]string{"NUMBER", "TEXT"},    // xdbcTypeName
			[]string{"-5", "12"},          // xdbcSqlDataType
			[]string{"1", "1"},            // xdbcNullable
			[]string{"YES", "YES"},        // xdbcIsNullable
			[]string{"0", "0"},            // xdbcScale
			[]string{"10", "0"},           // xdbcNumPrecRadix
			[]string{"38", "16777216"},    // xdbcCharMaxLen (xdbcPrecision)
			[]string{"0", "16777216"},     // xdbcCharOctetLen
			[]string{"-5", "12", "0"},     // xdbcDateTimeSub
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			rdr, err := suite.cnxn.GetObjects(suite.ctx, adbc.ObjectDepthColumns, nil, nil, nil, nil, nil)
			suite.Require().NoError(err)
			defer rdr.Release()

			suite.Truef(adbc.GetObjectsSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", adbc.GetObjectsSchema, rdr.Schema())
			suite.True(rdr.Next())
			rec := rdr.Record()
			suite.Greater(rec.NumRows(), int64(0))
			var (
				foundExpected        = false
				catalogDbSchemasList = rec.Column(1).(*array.List)
				catalogDbSchemas     = catalogDbSchemasList.ListValues().(*array.Struct)
				dbSchemaNames        = catalogDbSchemas.Field(0).(*array.String)
				dbSchemaTablesList   = catalogDbSchemas.Field(1).(*array.List)
				dbSchemaTables       = dbSchemaTablesList.ListValues().(*array.Struct)
				tableColumnsList     = dbSchemaTables.Field(2).(*array.List)
				tableColumns         = tableColumnsList.ListValues().(*array.Struct)

				colnames          = make([]string, 0)
				positions         = make([]string, 0)
				comments          = make([]string, 0)
				xdbcDataTypes     = make([]string, 0)
				dataTypes         = make([]string, 0)
				xdbcTypeNames     = make([]string, 0)
				xdbcCharMaxLens   = make([]string, 0)
				xdbcScales        = make([]string, 0)
				xdbcNumPrecRadixs = make([]string, 0)
				xdbcNullables     = make([]string, 0)
				xdbcSqlDataTypes  = make([]string, 0)
				xdbcDateTimeSub   = make([]string, 0)
				xdbcCharOctetLen  = make([]string, 0)
				xdbcIsNullables   = make([]string, 0)
			)
			for row := 0; row < int(rec.NumRows()); row++ {
				dbSchemaIdxStart, dbSchemaIdxEnd := catalogDbSchemasList.ValueOffsets(row)
				for dbSchemaIdx := dbSchemaIdxStart; dbSchemaIdx < dbSchemaIdxEnd; dbSchemaIdx++ {
					schemaName := dbSchemaNames.Value(int(dbSchemaIdx))
					tblIdxStart, tblIdxEnd := dbSchemaTablesList.ValueOffsets(int(dbSchemaIdx))
					for tblIdx := tblIdxStart; tblIdx < tblIdxEnd; tblIdx++ {
						tableName := dbSchemaTables.Field(0).(*array.String).Value(int(tblIdx))

						if strings.EqualFold(schemaName, suite.Quirks.DBSchema()) && strings.EqualFold("bulk_ingest", tableName) {
							foundExpected = true

							colIdxStart, colIdxEnd := tableColumnsList.ValueOffsets(int(tblIdx))
							for colIdx := colIdxStart; colIdx < colIdxEnd; colIdx++ {
								name := tableColumns.Field(0).(*array.String).Value(int(colIdx))
								colnames = append(colnames, strings.ToLower(name))

								pos := tableColumns.Field(1).(*array.Int32).Value(int(colIdx))
								positions = append(positions, strconv.Itoa(int(pos)))

								comments = append(comments, tableColumns.Field(2).(*array.String).Value(int(colIdx)))

								xdt := tableColumns.Field(3).(*array.Int16).Value(int(colIdx))
								xdbcDataTypes = append(xdbcDataTypes, strconv.Itoa(int(xdt)))

								dataType := tableColumns.Field(4).(*array.String).Value(int(colIdx))
								dataTypes = append(dataTypes, dataType)
								xdbcTypeNames = append(xdbcTypeNames, dataType)

								// these are column size attributes used for either precision for numbers OR the length for text
								maxLenOrPrecision := tableColumns.Field(5).(*array.Int32).Value(int(colIdx))
								xdbcCharMaxLens = append(xdbcCharMaxLens, strconv.Itoa(int(maxLenOrPrecision)))

								scale := tableColumns.Field(6).(*array.Int16).Value(int(colIdx))
								xdbcScales = append(xdbcScales, strconv.Itoa(int(scale)))

								radix := tableColumns.Field(7).(*array.Int16).Value(int(colIdx))
								xdbcNumPrecRadixs = append(xdbcNumPrecRadixs, strconv.Itoa(int(radix)))

								isnull := tableColumns.Field(8).(*array.Int16).Value(int(colIdx))
								xdbcNullables = append(xdbcNullables, strconv.Itoa(int(isnull)))

								sqlType := tableColumns.Field(10).(*array.Int16).Value(int(colIdx))
								xdbcSqlDataTypes = append(xdbcSqlDataTypes, strconv.Itoa(int(sqlType)))

								dtPrec := tableColumns.Field(11).(*array.Int16).Value(int(colIdx))
								xdbcDateTimeSub = append(xdbcSqlDataTypes, strconv.Itoa(int(dtPrec)))

								charOctetLen := tableColumns.Field(12).(*array.Int32).Value(int(colIdx))
								xdbcCharOctetLen = append(xdbcCharOctetLen, strconv.Itoa(int(charOctetLen)))

								xdbcIsNullables = append(xdbcIsNullables, tableColumns.Field(13).(*array.String).Value(int(colIdx)))
							}
						}
					}
				}
			}

			suite.False(rdr.Next())
			suite.True(foundExpected)
			suite.Equal(tt.colnames, colnames)                  // colNames
			suite.Equal(tt.positions, positions)                // positions
			suite.Equal(tt.comments, comments)                  // comments
			suite.Equal(tt.xdbcDataType, xdbcDataTypes)         // xdbcDataType
			suite.Equal(tt.dataTypes, dataTypes)                // dataTypes
			suite.Equal(tt.xdbcTypeName, xdbcTypeNames)         // xdbcTypeName
			suite.Equal(tt.xdbcCharMaxLen, xdbcCharMaxLens)     // xdbcCharMaxLen
			suite.Equal(tt.xdbcScale, xdbcScales)               // xdbcScale
			suite.Equal(tt.xdbcNumPrecRadix, xdbcNumPrecRadixs) // xdbcNumPrecRadix
			suite.Equal(tt.xdbcNullable, xdbcNullables)         // xdbcNullable
			suite.Equal(tt.xdbcSqlDataType, xdbcSqlDataTypes)   // xdbcSqlDataType
			suite.Equal(tt.xdbcDateTimeSub, xdbcDateTimeSub)    // xdbcDateTimeSub
			suite.Equal(tt.xdbcCharOctetLen, xdbcCharOctetLen)  // xdbcCharOctetLen
			suite.Equal(tt.xdbcIsNullable, xdbcIsNullables)     // xdbcIsNullable

		})
	}
}

func (suite *SnowflakeTests) TestNewDatabaseGetSetOptions() {
	key1, val1 := "key1", "val1"
	key2, val2 := "key2", "val2"

	db, err := suite.driver.NewDatabase(map[string]string{
		key1: val1,
		key2: val2,
	})
	suite.NoError(err)
	suite.NotNil(db)
	defer suite.NoError(db.Close())

	getSetDB, ok := db.(adbc.GetSetOptions)
	suite.True(ok)

	optVal1, err := getSetDB.GetOption(key1)
	suite.NoError(err)
	suite.Equal(optVal1, val1)
	optVal2, err := getSetDB.GetOption(key2)
	suite.NoError(err)
	suite.Equal(optVal2, val2)
}

func (suite *SnowflakeTests) TestTimestampSnow() {
	suite.Require().NoError(suite.stmt.SetSqlQuery(`ALTER SESSION SET TIMEZONE = "America/New_York"`))
	_, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery("SHOW WAREHOUSES"))
	rdr, _, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.True(rdr.Next())
	rec := rdr.Record()
	for _, f := range rec.Schema().Fields() {
		st, ok := f.Metadata.GetValue("SNOWFLAKE_TYPE")
		if !ok {
			continue
		}
		if st == "timestamp_ltz" {
			suite.Require().IsType(&arrow.TimestampType{}, f.Type)
			suite.Equal("America/New_York", f.Type.(*arrow.TimestampType).TimeZone)
		}
	}
}

func (suite *SnowflakeTests) TestUseHighPrecision() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "NUMBERTYPETEST"))

	suite.Require().NoError(suite.stmt.SetSqlQuery(`CREATE OR REPLACE TABLE NUMBERTYPETEST (
		NUMBERDECIMAL NUMBER(38,0),
		NUMBERFLOAT NUMBER(15,2)
	)`))
	_, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`INSERT INTO NUMBERTYPETEST (NUMBERDECIMAL, NUMBERFLOAT)
		VALUES (1, 1234567.894), (12345678901234567890123456789012345678, 9876543210.987)`))
	_, err = suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueEnabled))
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT * FROM NUMBERTYPETEST"))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(2, n)
	suite.Truef(arrow.TypeEqual(&arrow.Decimal128Type{Precision: 38, Scale: 0}, rdr.Schema().Field(0).Type), "expected decimal(38, 0), got %s", rdr.Schema().Field(0).Type)
	suite.Truef(arrow.TypeEqual(&arrow.Decimal128Type{Precision: 15, Scale: 2}, rdr.Schema().Field(1).Type), "expected decimal(15, 2), got %s", rdr.Schema().Field(1).Type)

	suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueDisabled))
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT * FROM NUMBERTYPETEST"))
	rdr, n, err = suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(2, n)
	suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Int64, rdr.Schema().Field(0).Type), "expected int64, got %s", rdr.Schema().Field(0).Type)
	suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Float64, rdr.Schema().Field(1).Type), "expected float64, got %s", rdr.Schema().Field(1).Type)
	suite.True(rdr.Next())
	rec := rdr.Record()

	suite.Equal(1234567.89, rec.Column(1).(*array.Float64).Value(0))
	suite.Equal(9876543210.99, rec.Column(1).(*array.Float64).Value(1))
}

func (suite *SnowflakeTests) TestDecimalHighPrecision() {
	for sign := 0; sign <= 1; sign++ {
		for scale := 0; scale <= 2; scale++ {
			for precision := 3; precision <= 38; precision++ {
				numberString := strings.Repeat("9", precision-scale) + "." + strings.Repeat("9", scale)
				if sign == 1 {
					numberString = "-" + numberString
				}
				query := "SELECT CAST('" + numberString + fmt.Sprintf("' AS NUMBER(%d, %d)) AS RESULT", precision, scale)
				number, err := decimal128.FromString(numberString, int32(precision), int32(scale))
				suite.NoError(err)

				suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueEnabled))
				suite.Require().NoError(suite.stmt.SetSqlQuery(query))
				rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
				suite.Require().NoError(err)
				defer rdr.Release()

				suite.EqualValues(1, n)
				suite.Truef(arrow.TypeEqual(&arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, rdr.Schema().Field(0).Type), "expected decimal(%d, %d), got %s", precision, scale, rdr.Schema().Field(0).Type)
				suite.True(rdr.Next())
				rec := rdr.Record()

				suite.Equal(number, rec.Column(0).(*array.Decimal128).Value(0))
			}
		}
	}
}

func (suite *SnowflakeTests) TestNonIntDecimalLowPrecision() {
	for sign := 0; sign <= 1; sign++ {
		for precision := 3; precision <= 38; precision++ {
			scale := 2
			numberString := strings.Repeat("9", precision-scale) + ".99"
			if sign == 1 {
				numberString = "-" + numberString
			}
			query := "SELECT CAST('" + numberString + fmt.Sprintf("' AS NUMBER(%d, %d)) AS RESULT", precision, scale)
			decimalNumber, err := decimal128.FromString(numberString, int32(precision), int32(scale))
			suite.NoError(err)
			number := decimalNumber.ToFloat64(int32(scale))

			suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueDisabled))
			suite.Require().NoError(suite.stmt.SetSqlQuery(query))
			rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
			suite.Require().NoError(err)
			defer rdr.Release()

			suite.EqualValues(1, n)
			suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Float64, rdr.Schema().Field(0).Type), "expected float64, got %s", rdr.Schema().Field(0).Type)
			suite.True(rdr.Next())
			rec := rdr.Record()

			value := rec.Column(0).(*array.Float64).Value(0)
			difference := math.Abs(number - value)
			suite.Truef(difference < 1e-13, "expected %f, got %f", number, value)
		}
	}
}

func (suite *SnowflakeTests) TestIntDecimalLowPrecision() {
	for sign := 0; sign <= 1; sign++ {
		for precision := 3; precision <= 38; precision++ {
			scale := 0
			numberString := strings.Repeat("9", precision-scale)
			if sign == 1 {
				numberString = "-" + numberString
			}
			query := "SELECT CAST('" + numberString + fmt.Sprintf("' AS NUMBER(%d, %d)) AS RESULT", precision, scale)
			decimalNumber, err := decimal128.FromString(numberString, int32(precision), int32(scale))
			suite.NoError(err)
			// The current behavior of the driver for decimal128 values too large to fit into 64 bits is to simply
			// return the low 64 bits of the value.
			number := int64(decimalNumber.LowBits())

			suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueDisabled))
			suite.Require().NoError(suite.stmt.SetSqlQuery(query))
			rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
			suite.Require().NoError(err)
			defer rdr.Release()

			suite.EqualValues(1, n)
			suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Int64, rdr.Schema().Field(0).Type), "expected int64, got %s", rdr.Schema().Field(0).Type)
			suite.True(rdr.Next())
			rec := rdr.Record()

			value := rec.Column(0).(*array.Int64).Value(0)
			suite.Equal(number, value)
		}
	}
}

func (suite *SnowflakeTests) TestDescribeOnly() {
	suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueEnabled))
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT CAST('9999.99' AS NUMBER(6, 2)) AS RESULT"))
	schema, err := suite.stmt.(adbc.StatementExecuteSchema).ExecuteSchema(suite.ctx)
	suite.Require().NoError(err)

	suite.Equal(1, len(schema.Fields()))
	suite.Equal("RESULT", schema.Field(0).Name)
	suite.Truef(arrow.TypeEqual(&arrow.Decimal128Type{Precision: 6, Scale: 2}, schema.Field(0).Type), "expected decimal(6, 2), got %s", schema.Field(0).Type)
}

func TestJwtAuthenticationUnencryptedValue(t *testing.T) {
	// test doesn't participate in SnowflakeTests because
	// JWT auth has a different behavior
	uri, ok := os.LookupEnv("SNOWFLAKE_URI")
	if !ok {
		t.Skip("Cannot find the `SNOWFLAKE_URI` value")
	}

	keyValue, ok := os.LookupEnv("SNOWFLAKE_TEST_PKCS8_VALUE")
	if !ok {
		t.Skip("Cannot find the `SNOWFLAKE_TEST_PKCS8_VALUE` value")
	}

	ConnectWithJwt(uri, keyValue, "")
}

func TestJwtAuthenticationEncryptedValue(t *testing.T) {
	// test doesn't participate in SnowflakeTests because
	// JWT auth has a different behavior
	uri, ok := os.LookupEnv("SNOWFLAKE_URI")
	if !ok {
		t.Skip("Cannot find the `SNOWFLAKE_URI` value")
	}

	keyValue, ok := os.LookupEnv("SNOWFLAKE_TEST_PKCS8_EN_VALUE")
	if !ok {
		t.Skip("Cannot find the `SNOWFLAKE_TEST_PKCS8_EN_VALUE` value")
	}

	passcode, ok := os.LookupEnv("SNOWFLAKE_TEST_PKCS8_PASS")
	if !ok {
		t.Skip("Cannot find the `SNOWFLAKE_TEST_PKCS8_PASS` value")
	}

	ConnectWithJwt(uri, keyValue, passcode)
}

func ConnectWithJwt(uri, keyValue, passcode string) {

	// Windows funkiness
	if runtime.GOOS == "windows" {
		keyValue = strings.ReplaceAll(keyValue, "\\r", "\r")
		keyValue = strings.ReplaceAll(keyValue, "\\n", "\n")
	}

	cfg, err := gosnowflake.ParseDSN(uri)
	if err != nil {
		panic(err)
	}

	opts := map[string]string{
		driver.OptionAccount:                 cfg.Account,
		adbc.OptionKeyUsername:               cfg.User,
		driver.OptionDatabase:                cfg.Database,
		driver.OptionSchema:                  cfg.Schema,
		driver.OptionAuthType:                driver.OptionValueAuthJwt,
		driver.OptionJwtPrivateKeyPkcs8Value: keyValue,
	}

	if cfg.Warehouse != "" {
		opts[driver.OptionWarehouse] = cfg.Warehouse
	}

	if cfg.Host != "" {
		opts[driver.OptionHost] = cfg.Host
	}

	// if doing encrypted
	if passcode != "" {
		opts[driver.OptionJwtPrivateKeyPkcs8Password] = passcode
	}

	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	adbcDriver := driver.NewDriver(mem)
	db, err := adbcDriver.NewDatabase(opts)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cnxn, err := db.Open(context.Background())
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()
}

func (suite *SnowflakeTests) TestJwtPrivateKey() {
	suite.T().Skipf("apache/arrow-adbc#1364")

	// grab the username from the DSN
	cfg, err := gosnowflake.ParseDSN(suite.Quirks.dsn)
	suite.NoError(err)
	username := cfg.User

	// write the generated RSA key out to a file
	writeKey := func(filename string, key []byte) string {
		f, err := os.CreateTemp("", filename)
		suite.NoError(err)
		_, err = f.Write(key)
		suite.NoError(err)
		return f.Name()
	}

	// set the Snowflake user's RSA public key
	setKey := func(privKey *rsa.PrivateKey) {
		suite.NoError(suite.stmt.SetSqlQuery("USE ROLE ACCOUNTADMIN"))
		_, err := suite.stmt.ExecuteUpdate(suite.ctx)
		suite.NoError(err)

		if privKey != nil {
			pubKeyBytes, err := x509.MarshalPKIXPublicKey(privKey.Public())
			suite.NoError(err)
			encodedKey := base64.StdEncoding.EncodeToString(pubKeyBytes)
			suite.NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("ALTER USER %s SET RSA_PUBLIC_KEY='%s'", username, encodedKey)))
		} else {
			suite.NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("ALTER USER %s SET RSA_PUBLIC_KEY=''", username)))
		}
		_, err = suite.stmt.ExecuteUpdate(suite.ctx)
		suite.NoError(err)
	}

	// open a new connection using JWT authentication and verify that a simple query runs
	verifyKey := func(keyFile string) {
		opts := suite.Quirks.DatabaseOptions()
		opts[driver.OptionAuthType] = driver.OptionValueAuthJwt
		opts[driver.OptionJwtPrivateKey] = keyFile
		db, err := suite.driver.NewDatabase(opts)
		suite.NoError(err)
		defer db.Close()
		cnxn, err := db.Open(suite.ctx)
		suite.NoError(err)
		defer cnxn.Close()
		stmt, err := cnxn.NewStatement()
		suite.NoError(err)
		defer stmt.Close()

		suite.NoError(stmt.SetSqlQuery("SELECT 1"))
		rdr, _, err := stmt.ExecuteQuery(suite.ctx)
		defer rdr.Release()
		suite.NoError(err)
	}

	// generate a key and set it the Snowflake user
	rsaKey, _ := rsa.GenerateKey(rand.Reader, 2048)
	setKey(rsaKey)

	// when the test concludes, reset the user's key
	defer setKey(nil)

	// PKCS1 key
	rsaKeyPem := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(rsaKey),
	})
	pkcs1Key := writeKey("key.pem", rsaKeyPem)
	defer os.Remove(pkcs1Key)
	verifyKey(pkcs1Key)

	// PKCS8 key
	rsaKeyP8Bytes, _ := x509.MarshalPKCS8PrivateKey(rsaKey)
	rsaKeyP8 := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: rsaKeyP8Bytes,
	})
	pkcs8Key := writeKey("key.p8", rsaKeyP8)
	defer os.Remove(pkcs8Key)
	verifyKey(pkcs8Key)

	// binary key
	block, _ := pem.Decode([]byte(rsaKeyPem))
	binKey := writeKey("key.bin", block.Bytes)
	defer os.Remove(binKey)
	verifyKey(binKey)
}
