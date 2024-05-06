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
	"bytes"
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
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/apache/arrow/go/v17/arrow/memory"
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

func quoteTblName(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

func (s *SnowflakeQuirks) CreateSampleTable(tableName string, r arrow.Record) error {
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE TABLE ")
	b.WriteString(quoteTblName(tableName))
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

	insertQuery := "INSERT INTO " + quoteTblName(tableName) + " VALUES ("
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

	if err = stmt.SetSqlQuery(`DROP TABLE IF EXISTS ` + quoteTblName(tblname)); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (s *SnowflakeQuirks) Alloc() memory.Allocator                     { return s.mem }
func (s *SnowflakeQuirks) BindParameter(_ int) string                  { return "?" }
func (s *SnowflakeQuirks) SupportsBulkIngest(string) bool              { return true }
func (s *SnowflakeQuirks) SupportsConcurrentStatements() bool          { return true }
func (s *SnowflakeQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (s *SnowflakeQuirks) SupportsExecuteSchema() bool                 { return true }
func (s *SnowflakeQuirks) SupportsGetSetOptions() bool                 { return true }
func (s *SnowflakeQuirks) SupportsPartitionedData() bool               { return false }
func (s *SnowflakeQuirks) SupportsStatistics() bool                    { return false }
func (s *SnowflakeQuirks) SupportsTransactions() bool                  { return true }
func (s *SnowflakeQuirks) SupportsGetParameterSchema() bool            { return false }
func (s *SnowflakeQuirks) SupportsDynamicParameterBinding() bool       { return false }
func (s *SnowflakeQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (s *SnowflakeQuirks) Catalog() string                             { return s.catalogName }
func (s *SnowflakeQuirks) DBSchema() string                            { return s.schemaName }
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
	case adbc.InfoVendorVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorArrowVersion:
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
	t.Cleanup(func() {
		dropTempSchema(uri, q.schemaName)
	})

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

	sessionTimezone := "UTC"
	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf(`ALTER SESSION SET TIMEZONE = "%s"`, sessionTimezone)))
	_, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

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

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest" ORDER BY "col" ASC NULLS FIRST`))
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

func (suite *SnowflakeTests) TestSqlIngestRecordAndStreamAreEquivalent() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_bind"))
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_bind_stream"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_float64", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name: "col_string", Type: arrow.BinaryTypes.String,
			Nullable: true,
		},
		{
			Name: "col_binary", Type: arrow.BinaryTypes.Binary,
			Nullable: true,
		},
		{
			Name: "col_boolean", Type: arrow.FixedWidthTypes.Boolean,
			Nullable: true,
		},
		{
			Name: "col_date32", Type: arrow.FixedWidthTypes.Date32,
			Nullable: true,
		},
		{
			Name: "col_time64ns", Type: arrow.FixedWidthTypes.Time64ns,
			Nullable: true,
		},
		{
			Name: "col_time64us", Type: arrow.FixedWidthTypes.Time64us,
			Nullable: true,
		},
		{
			Name: "col_time32ms", Type: arrow.FixedWidthTypes.Time32ms,
			Nullable: true,
		},
		{
			Name: "col_time32s", Type: arrow.FixedWidthTypes.Time32s,
			Nullable: true,
		},
		{
			Name: "col_timestamp_ns", Type: arrow.FixedWidthTypes.Timestamp_ns,
			Nullable: true,
		},
		{
			Name: "col_timestamp_us", Type: arrow.FixedWidthTypes.Timestamp_us,
			Nullable: true,
		},
		{
			Name: "col_timestamp_s", Type: arrow.FixedWidthTypes.Timestamp_s,
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{-1, 0, 25}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{-1.1, 0, 25.95}, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"first", "second", "third"}, nil)
	bldr.Field(3).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("first"), []byte("second"), []byte("third")}, nil)
	bldr.Field(4).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	bldr.Field(5).(*array.Date32Builder).AppendValues([]arrow.Date32{1, 2, 3}, nil)
	bldr.Field(6).(*array.Time64Builder).AppendValues([]arrow.Time64{1, 2, 3}, nil)
	bldr.Field(7).(*array.Time64Builder).AppendValues([]arrow.Time64{1, 2, 3}, nil)
	bldr.Field(8).(*array.Time32Builder).AppendValues([]arrow.Time32{1, 2, 3}, nil)
	bldr.Field(9).(*array.Time32Builder).AppendValues([]arrow.Time32{1, 2, 3}, nil)
	bldr.Field(10).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(11).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(12).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	stream, err := array.NewRecordReader(sc, []arrow.Record{rec})
	suite.Require().NoError(err)
	defer stream.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_bind"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_bind" ORDER BY "col_int64" ASC`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	resultBind := rdr.Record()

	// New session to clean up TEMPORARY resources in Snowflake associated with the previous one
	suite.NoError(suite.stmt.Close())
	suite.NoError(suite.cnxn.Close())
	suite.cnxn, err = suite.db.Open(suite.ctx)
	suite.NoError(err)
	suite.stmt, err = suite.cnxn.NewStatement()
	suite.NoError(err)

	suite.Require().NoError(suite.stmt.BindStream(suite.ctx, stream))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_bind_stream"))
	n, err = suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_bind_stream" ORDER BY "col_int64" ASC`))
	rdr, n, err = suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	resultBindStream := rdr.Record()

	suite.Truef(array.RecordEqual(resultBind, resultBindStream), "expected: %s\ngot: %s", resultBind, resultBindStream)
	suite.False(rdr.Next())

	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestSqlIngestRoundtripTypes() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_roundtrip"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_float64", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name: "col_string", Type: arrow.BinaryTypes.String,
			Nullable: true,
		},
		{
			Name: "col_binary", Type: arrow.BinaryTypes.Binary,
			Nullable: true,
		},
		{
			Name: "col_boolean", Type: arrow.FixedWidthTypes.Boolean,
			Nullable: true,
		},
		{
			Name: "col_date32", Type: arrow.FixedWidthTypes.Date32,
			Nullable: true,
		},
		{
			Name: "col_time64ns", Type: arrow.FixedWidthTypes.Time64ns,
			Nullable: true,
		},
		{
			Name: "col_time64us", Type: arrow.FixedWidthTypes.Time64us,
			Nullable: true,
		},
		{
			Name: "col_time32ms", Type: arrow.FixedWidthTypes.Time32ms,
			Nullable: true,
		},
		{
			Name: "col_time32s", Type: arrow.FixedWidthTypes.Time32s,
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{-1, 0, 25}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{-1.1, 0, 25.95}, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"first", "second", "third"}, nil)
	bldr.Field(3).(*array.BinaryBuilder).AppendValues([][]byte{[]byte("first"), []byte("second"), []byte("third")}, nil)
	bldr.Field(4).(*array.BooleanBuilder).AppendValues([]bool{true, false, true}, nil)
	bldr.Field(5).(*array.Date32Builder).AppendValues([]arrow.Date32{1, 2, 3}, nil)
	bldr.Field(6).(*array.Time64Builder).AppendValues([]arrow.Time64{1, 2, 3}, nil)
	bldr.Field(7).(*array.Time64Builder).AppendValues([]arrow.Time64{1, 2, 3}, nil)
	bldr.Field(8).(*array.Time32Builder).AppendValues([]arrow.Time32{1, 2, 3}, nil)
	bldr.Field(9).(*array.Time32Builder).AppendValues([]arrow.Time32{1, 2, 3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_roundtrip"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_roundtrip" ORDER BY "col_int64" ASC`))
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

func (suite *SnowflakeTests) TestSqlIngestTimestampTypes() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_timestamps"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sessionTimezone := "America/Phoenix"
	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf(`ALTER SESSION SET TIMEZONE = "%s"`, sessionTimezone)))
	_, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_timestamp_ns", Type: arrow.FixedWidthTypes.Timestamp_ns,
			Nullable: true,
		},
		{
			Name: "col_timestamp_us", Type: arrow.FixedWidthTypes.Timestamp_us,
			Nullable: true,
		},
		{
			Name: "col_timestamp_ms", Type: arrow.FixedWidthTypes.Timestamp_ms,
			Nullable: true,
		},
		{
			Name: "col_timestamp_s", Type: arrow.FixedWidthTypes.Timestamp_s,
			Nullable: true,
		},
		{
			Name: "col_timestamp_s_tz", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "EST"},
			Nullable: true,
		},
		{
			Name: "col_timestamp_s_ntz", Type: &arrow.TimestampType{Unit: arrow.Second},
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(2).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(4).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(5).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(6).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_timestamps"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_timestamps" ORDER BY "col_int64" ASC`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_timestamp_ns", Type: &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: sessionTimezone},
			Nullable: true,
		},
		{
			Name: "col_timestamp_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: sessionTimezone},
			Nullable: true,
		},
		{
			Name: "col_timestamp_ms", Type: &arrow.TimestampType{Unit: arrow.Millisecond, TimeZone: sessionTimezone},
			Nullable: true,
		},
		{
			Name: "col_timestamp_s", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: sessionTimezone},
			Nullable: true,
		},
		{
			Name: "col_timestamp_s_tz", Type: &arrow.TimestampType{Unit: arrow.Second, TimeZone: sessionTimezone},
			Nullable: true,
		},
		{
			Name: "col_timestamp_s_ntz", Type: &arrow.TimestampType{Unit: arrow.Second},
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(mem, expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_timestamp_ns": 1,
			"col_timestamp_us": 1,
			"col_timestamp_ms": 1,
			"col_timestamp_s": 1,
			"col_timestamp_s_tz": 1,
			"col_timestamp_s_ntz": 1
		},
		{
			"col_int64": 2,
			"col_timestamp_ns": 2,
			"col_timestamp_us": 2,
			"col_timestamp_ms": 2,
			"col_timestamp_s": 2,
			"col_timestamp_s_tz": 2,
			"col_timestamp_s_ntz": 2
		},
		{
			"col_int64": 3,
			"col_timestamp_ns": 3,
			"col_timestamp_us": 3,
			"col_timestamp_ms": 3,
			"col_timestamp_s": 3,
			"col_timestamp_s_tz": 3,
			"col_timestamp_s_ntz": 3
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestSqlIngestDate64Type() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_date64"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_date64", Type: arrow.FixedWidthTypes.Date64,
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.Date64Builder).AppendValues([]arrow.Date64{86400000, 172800000, 259200000}, nil) // 1,2,3 days of milliseconds

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_date64"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_date64" ORDER BY "col_int64" ASC`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_date64", Type: arrow.FixedWidthTypes.Date32,
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(mem, expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_date64": 1
		},
		{
			"col_int64": 2,
			"col_date64": 2
		},
		{
			"col_int64": 3,
			"col_date64": 3
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestSqlIngestHighPrecision() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_high_precision"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_float64", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name: "col_decimal128_whole", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0},
			Nullable: true,
		},
		{
			Name: "col_decimal128_fractional", Type: &arrow.Decimal128Type{Precision: 38, Scale: 2},
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{1.2, 2.34, 3.456}, nil)
	bldr.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(123), decimal128.FromI64(456), decimal128.FromI64(789)}, nil)
	num1, err := decimal128.FromString("123", 38, 2)
	suite.Require().NoError(err)
	num2, err := decimal128.FromString("456.7", 38, 2)
	suite.Require().NoError(err)
	num3, err := decimal128.FromString("891.01", 38, 2)
	suite.Require().NoError(err)
	bldr.Field(3).(*array.Decimal128Builder).AppendValues([]decimal128.Num{num1, num2, num3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_high_precision"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_high_precision" ORDER BY "col_int64" ASC`))
	suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueEnabled))
	defer func() {
		suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueDisabled))
	}()
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{ // INT64 -> DECIMAL(38, 0) on roundtrip
			Name: "col_int64", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0},
			Nullable: true,
		},
		{ // Preserved on roundtrip
			Name: "col_float64", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{ // Preserved on roundtrip
			Name: "col_decimal128_whole", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0},
			Nullable: true,
		},
		{ // Preserved on roundtrip
			Name: "col_decimal128_fractional", Type: &arrow.Decimal128Type{Precision: 38, Scale: 2},
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(mem, expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_float64": 1.2,
			"col_decimal128_whole": 123,
			"col_decimal128_fractional": 123.00
		},
		{
			"col_int64": 2,
			"col_float64": 2.34,
			"col_decimal128_whole": 456,
			"col_decimal128_fractional": 456.70
		},
		{
			"col_int64": 3,
			"col_float64": 3.456,
			"col_decimal128_whole": 789,
			"col_decimal128_fractional": 891.01
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestSqlIngestLowPrecision() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_high_precision"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_float64", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name: "col_decimal128_whole", Type: &arrow.Decimal128Type{Precision: 38, Scale: 0},
			Nullable: true,
		},
		{
			Name: "col_decimal128_fractional", Type: &arrow.Decimal128Type{Precision: 38, Scale: 2},
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues([]float64{1.2, 2.34, 3.456}, nil)
	bldr.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(123), decimal128.FromI64(456), decimal128.FromI64(789)}, nil)
	num1, err := decimal128.FromString("123", 38, 2)
	suite.Require().NoError(err)
	num2, err := decimal128.FromString("456.7", 38, 2)
	suite.Require().NoError(err)
	num3, err := decimal128.FromString("891.01", 38, 2)
	suite.Require().NoError(err)
	bldr.Field(3).(*array.Decimal128Builder).AppendValues([]decimal128.Num{num1, num2, num3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_high_precision"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_high_precision" ORDER BY "col_int64" ASC`))
	// OptionUseHighPrecision already disabled
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{ // Preserved on roundtrip
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{ // Preserved on roundtrip
			Name: "col_float64", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{ // DECIMAL(38, 0) -> INT64 on roundtrip
			Name: "col_decimal128_whole", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{ // DECIMAL(38, 2) -> FLOAT64 on roundtrip
			Name: "col_decimal128_fractional", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(mem, expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_float64": 1.2,
			"col_decimal128_whole": 123,
			"col_decimal128_fractional": 123.00
		},
		{
			"col_int64": 2,
			"col_float64": 2.34,
			"col_decimal128_whole": 456,
			"col_decimal128_fractional": 456.70
		},
		{
			"col_int64": 3,
			"col_float64": 3.456,
			"col_decimal128_whole": 789,
			"col_decimal128_fractional": 891.01
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestSqlIngestStructType() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_struct"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_struct", Type: arrow.StructOf([]arrow.Field{
				{Name: "name", Type: arrow.BinaryTypes.String},
				{Name: "age", Type: arrow.PrimitiveTypes.Int64},
			}...),
			Nullable: true,
		},
		{
			Name: "col_struct_of_struct", Type: arrow.StructOf([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64},
				{Name: "nested", Type: arrow.StructOf([]arrow.Field{
					{Name: "nested_id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "ready", Type: arrow.FixedWidthTypes.Boolean},
				}...)},
			}...),
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	struct1bldr := bldr.Field(1).(*array.StructBuilder)
	struct1bldr.AppendValues([]bool{true, true, true})
	struct1bldr.FieldBuilder(0).(*array.StringBuilder).AppendValues([]string{"one", "two", "three"}, nil)
	struct1bldr.FieldBuilder(1).(*array.Int64Builder).AppendValues([]int64{10, 20, 30}, nil)

	struct2bldr := bldr.Field(2).(*array.StructBuilder)
	struct2bldr.AppendValues([]bool{true, false, true})
	struct2bldr.FieldBuilder(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3}, nil)

	struct3bldr := struct2bldr.FieldBuilder(1).(*array.StructBuilder)
	struct3bldr.AppendValues([]bool{true, false, true})
	struct3bldr.FieldBuilder(0).(*array.Int64Builder).AppendValues([]int64{1, 0, 3}, nil)
	struct3bldr.FieldBuilder(1).(*array.BooleanBuilder).AppendValues([]bool{true, false, false}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_struct"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_struct" ORDER BY "col_int64" ASC`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_struct", Type: arrow.BinaryTypes.String,
			Nullable: true,
		},
		{
			Name: "col_struct_of_struct", Type: arrow.BinaryTypes.String,
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(mem, expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_struct": "{\n  \"age\": 10,\n  \"name\": \"one\"\n}",
			"col_struct_of_struct": "{\n  \"id\": 1,\n  \"nested\": {\n    \"nested_id\": 1,\n    \"ready\": true\n  }\n}"
		},
		{
			"col_int64": 2,
			"col_struct": "{\n  \"age\": 20,\n  \"name\": \"two\"\n}"
		},
		{
			"col_int64": 3,
			"col_struct": "{\n  \"age\": 30,\n  \"name\": \"three\"\n}",
			"col_struct_of_struct": "{\n  \"id\": 3,\n  \"nested\": {\n    \"nested_id\": 3,\n    \"ready\": false\n  }\n}"
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)
	logicalTypeStruct, ok := result.Schema().Field(1).Metadata.GetValue("logicalType")
	suite.True(ok)
	suite.Equal("OBJECT", logicalTypeStruct)
	logicalTypeStructStruct, ok := result.Schema().Field(2).Metadata.GetValue("logicalType")
	suite.True(ok)
	suite.Equal("OBJECT", logicalTypeStructStruct)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestSqlIngestMapType() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_map"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_map", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int64),
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	mapbldr := bldr.Field(1).(*array.MapBuilder)
	keybldr := mapbldr.KeyBuilder().(*array.StringBuilder)
	itembldr := mapbldr.ItemBuilder().(*array.Int64Builder)

	mapbldr.Append(true)
	keybldr.Append("key1")
	itembldr.Append(1)
	// keybldr.Append("key1a") TODO(joellubi): Snowflake returns 'SQL execution internal error', seemingly for repetition levels > 0
	// itembldr.Append(11)
	mapbldr.Append(true)
	keybldr.Append("key2")
	itembldr.Append(2)
	mapbldr.Append(true)
	keybldr.Append("key3")
	itembldr.Append(3)

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_map"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_map" ORDER BY "col_int64" ASC`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_map", Type: arrow.BinaryTypes.String,
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(mem, expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_map": "{\n  \"key_value\": [\n    {\n      \"key\": \"key1\",\n      \"value\": 1\n    }\n  ]\n}"
		},
		{
			"col_int64": 2,
			"col_map": "{\n  \"key_value\": [\n    {\n      \"key\": \"key2\",\n      \"value\": 2\n    }\n  ]\n}"
		},
		{
			"col_int64": 3,
			"col_map": "{\n  \"key_value\": [\n    {\n      \"key\": \"key3\",\n      \"value\": 3\n    }\n  ]\n}"
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)
	logicalTypeMap, ok := result.Schema().Field(1).Metadata.GetValue("logicalType")
	suite.True(ok)
	suite.Equal("OBJECT", logicalTypeMap)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestSqlIngestListType() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_list"))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(suite.T(), 0)

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_list", Type: arrow.ListOf(arrow.BinaryTypes.String),
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(mem, sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	listbldr := bldr.Field(1).(*array.ListBuilder)
	listvalbldr := listbldr.ValueBuilder().(*array.StringBuilder)
	listbldr.Append(true)
	listvalbldr.Append("one")
	// listvalbldr.Append("one2") TODO(joellubi): Snowflake returns 'SQL execution internal error', seemingly for repetition levels > 0
	listbldr.Append(true)
	listvalbldr.Append("two")
	listbldr.Append(true)
	listvalbldr.Append("three")

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.stmt.Bind(suite.ctx, rec))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_list"))
	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(3, n)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest_list" ORDER BY "col_int64" ASC`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_list", Type: arrow.BinaryTypes.String,
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(mem, expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_list": "[\n  \"one\"\n]"
		},
		{
			"col_int64": 2,
			"col_list": "[\n  \"two\"\n]"
		},
		{
			"col_int64": 3,
			"col_list": "[\n  \"three\"\n]"
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)
	logicalTypeList, ok := result.Schema().Field(1).Metadata.GetValue("logicalType")
	suite.True(ok)
	suite.Equal("ARRAY", logicalTypeList)

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

	// Snowflake may add more columns to the result of this query in the future,
	// so we just test a subset known to be supported at the time of writing
	expectedFieldNames := []string{
		"name",
		"state",
		"type",
		"size",
		"running",
		"queued",
		"is_default",
		"is_current",
		"auto_suspend",
		"auto_resume",
		"available",
		"provisioning",
		"quiescing",
		"other",
		"created_on",
		"resumed_on",
		"updated_on",
		"owner",
		"comment",
		"resource_monitor",
		"actives",
		"pendings",
		"failed",
		"suspended",
		"uuid",
		"budget",
		"owner_role_type",
	}

	fields := rec.Schema().Fields()
outer: // Check that each of the expected field names are in the result schema
	for _, expectedFieldName := range expectedFieldNames {
		for _, f := range fields {
			if f.Name == expectedFieldName {
				continue outer
			}
		}
		suite.Failf("unexpected result schema", "column '%s' expected but not found", expectedFieldName)
	}

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

func (suite *SnowflakeTests) TestAdditionalDriverInfo() {
	rdr, err := suite.cnxn.GetInfo(
		suite.ctx,
		[]adbc.InfoCode{
			adbc.InfoVendorSql,
			adbc.InfoVendorSubstrait,
		},
	)
	suite.Require().NoError(err)

	var totalRows int64
	for rdr.Next() {
		rec := rdr.Record()
		totalRows += rec.NumRows()
		code := rec.Column(0).(*array.Uint32)
		info := rec.Column(1).(*array.DenseUnion)

		for i := 0; i < int(rec.NumRows()); i++ {
			if code.Value(i) == uint32(adbc.InfoVendorSql) {
				arr, ok := info.Field(info.ChildID(i)).(*array.Boolean)
				suite.Require().True(ok)
				suite.Require().Equal(true, arr.Value(i))
			}
			if code.Value(i) == uint32(adbc.InfoVendorSubstrait) {
				arr, ok := info.Field(info.ChildID(i)).(*array.Boolean)
				suite.Require().True(ok)
				suite.Require().Equal(false, arr.Value(i))
			}
		}
	}
	suite.Require().Equal(int64(2), totalRows)
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

func (suite *SnowflakeTests) TestMetadataOnlyQuery() {
	// force more than one chunk for `SHOW FUNCTIONS` which will return
	// JSON data instead of arrow, even though we ask for Arrow
	suite.Require().NoError(suite.stmt.SetSqlQuery(`ALTER SESSION SET CLIENT_RESULT_CHUNK_SIZE = 50`))
	_, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

	// since we lowered the CLIENT_RESULT_CHUNK_SIZE this will return at least
	// 1 chunk in addition to the first one. Metadata queries will return JSON
	// no matter what currently.
	suite.Require().NoError(suite.stmt.SetSqlQuery(`SHOW FUNCTIONS`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	recv := int64(0)
	for rdr.Next() {
		recv += rdr.Record().NumRows()
	}

	// verify that we got the exepected number of rows if we sum up
	// all the rows from each record in the stream.
	suite.Equal(n, recv)
}

func (suite *SnowflakeTests) TestEmptyResultSet() {
	// regression test for apache/arrow-adbc#1804
	// this would previously crash
	suite.Require().NoError(suite.stmt.SetSqlQuery(`SELECT 42 WHERE 1=0`))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	recv := int64(0)
	for rdr.Next() {
		recv += rdr.Record().NumRows()
	}

	// verify that we got the exepected number of rows if we sum up
	// all the rows from each record in the stream.
	suite.Equal(n, recv)
	suite.Equal(recv, int64(0))
}
