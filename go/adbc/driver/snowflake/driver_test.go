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
	sqldriver "database/sql/driver"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/google/uuid"
	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SnowflakeQuirks struct {
	dsn         string
	mem         *memory.CheckedAllocator
	connector   sqldriver.Connector
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

func (s *SnowflakeQuirks) CreateSampleTable(tableName string, r arrow.Record) (err error) {
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
	defer func() {
		err = errors.Join(err, db.Close())
	}()

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

	_, err = db.Exec(insertQuery, args...)
	return err
}

func (s *SnowflakeQuirks) DropTable(cnxn adbc.Connection, tblname string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer func() {
		if err = stmt.Close(); err != nil {
			panic(err)
		}
	}()

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
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()

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
	defer func() {
		if err = db.Close(); err != nil {
			panic(err)
		}
	}()

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

func generateTraceparent() string {
	traceID := make([]byte, 16)
	_, _ = rand.Read(traceID)

	spanID := make([]byte, 8)
	_, _ = rand.Read(spanID)

	return fmt.Sprintf("00-%s-%s-01", hex.EncodeToString(traceID), hex.EncodeToString(spanID))
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

func (suite *SnowflakeTests) SetupTest() {
	var err error
	suite.ctx = context.Background()
	suite.driver = suite.Quirks.SetupDriver(suite.T())
	suite.db, err = suite.driver.NewDatabase(suite.Quirks.DatabaseOptions())
	suite.NoError(err)
	suite.cnxn, err = suite.db.Open(suite.ctx)
	suite.NoError(err)
	suite.stmt, err = suite.cnxn.NewStatement()
	suite.NoError(err)
}

func (suite *SnowflakeTests) TearDownTest() {
	suite.NoError(suite.stmt.Close())
	suite.NoError(suite.cnxn.Close())
	suite.Quirks.TearDownDriver(suite.T(), suite.driver)
	suite.cnxn = nil
	suite.NoError(suite.db.Close())
	suite.db = nil
	suite.driver = nil
}

type customTransport struct {
	base   *http.Transport
	called bool
}

func (t *customTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	t.called = true
	return t.base.RoundTrip(r)
}

func (suite *SnowflakeTests) TestNewDatabaseWithOptions() {
	t := suite.T()

	drv := suite.Quirks.SetupDriver(t).(driver.Driver)

	t.Run("WithTransporter", func(t *testing.T) {
		transport := &customTransport{base: gosnowflake.SnowflakeTransport}
		dbOptions := suite.Quirks.DatabaseOptions()
		// Add trace parent to the options.
		dbOptions[adbc.OptionKeyTelemetryTraceParent] = generateTraceparent()
		db, err := drv.NewDatabaseWithOptions(dbOptions,
			driver.WithTransporter(transport))
		suite.NoError(err)
		suite.NotNil(db)
		cnxn, err := db.Open(suite.ctx)
		suite.NoError(err)

		// Confirm database trace parent is non-empty and propagated to the connection trace parent
		dbImpl, ok := db.(driverbase.DatabaseImpl)
		suite.True(ok, "expecting db to implement interface 'driverbase.DatabaseImpl'")
		dTp := dbImpl.Base().GetTraceParent()
		cnxnImpl, ok := cnxn.(driverbase.ConnectionImpl)
		suite.True(ok, "expecting cnxn to implement interface 'driverbase.ConnectionImpl'")
		cTp := cnxnImpl.Base().GetTraceParent()
		suite.NotEmpty(t, dTp)
		suite.NotEmpty(t, cTp)
		suite.Equal(dTp, cTp, "expecting database and connection trace parent to be equal")

		suite.NoError(db.Close())
		suite.NoError(cnxn.Close())
		suite.True(transport.called)
	})
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_map": "{\n  \"key1\": 1\n}"
		},
		{
			"col_int64": 2,
			"col_map": "{\n  \"key2\": 2\n}"
		},
		{
			"col_int64": 3,
			"col_map": "{\n  \"key3\": 3\n}"
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

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
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

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
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

	rec, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil), strings.NewReader(`[
			{"int64s": 42, "strings": "foo"},
			{"int64s": -42, "strings": null},
			{"int64s": 0, "strings": ""}
		]`))
	suite.Require().NoError(err)
	defer rec.Release()

	suite.Require().NoError(suite.Quirks.CreateSampleTable("bulk_ingest", rec))
	suite.Require().NoError(suite.Quirks.CreateSampleTable("bulk_ingest2", rec))

	db := sql.OpenDB(suite.Quirks.connector)
	defer validation.CheckedClose(suite.T(), db)

	_, err = db.ExecContext(suite.ctx, `ALTER TABLE "bulk_ingest2" ADD CONSTRAINT bulk_ingest2_pk PRIMARY KEY (int64s, strings)`)
	suite.Require().NoError(err)

	_, err = db.ExecContext(suite.ctx, `ALTER TABLE "bulk_ingest" ADD CONSTRAINT bulk_ingest_pk PRIMARY KEY (int64s, strings)`)
	suite.Require().NoError(err)

	_, err = db.ExecContext(suite.ctx, `ALTER TABLE "bulk_ingest" ADD CONSTRAINT uniq_col UNIQUE (int64s)`)
	suite.Require().NoError(err)

	_, err = db.ExecContext(suite.ctx, `ALTER TABLE "bulk_ingest" ADD CONSTRAINT bulk_ingest_fk FOREIGN KEY (int64s, strings) REFERENCES "bulk_ingest2"`)
	suite.Require().NoError(err)

	var (
		expectedColnames    = []string{"int64s", "strings"}
		expectedPositions   = []string{"1", "2"}
		expectedDataTypes   = []string{"NUMBER", "TEXT"}
		expectedComments    = []string{"", ""}
		expectedConstraints = []struct {
			Name, Type string
		}{
			{Name: "BULK_INGEST_PK", Type: "PRIMARY KEY"},
			{Name: "BULK_INGEST_FK", Type: "FOREIGN KEY"},
			{Name: "UNIQ_COL", Type: "UNIQUE"},
		}
		expectedXdbcDataType     = []string{"9", "13"}
		expectedXdbcTypeName     = []string{"NUMBER", "TEXT"}
		expectedXdbcSqlDataType  = []string{"-5", "12"}
		expectedXdbcNullable     = []string{"1", "1"}
		expectedXdbcIsNullable   = []string{"YES", "YES"}
		expectedXdbcScale        = []string{"0", "0"}
		expectedXdbcNumPrecRadix = []string{"10", "0"}
		expectedXdbcCharMaxLen   = []string{"38", "16777216"}
		expectedXdbcCharOctetLen = []string{"0", "16777216"}
		expectedXdbcDateTimeSub  = []string{"0", "0"}
	)

	rdr, err := suite.cnxn.GetObjects(suite.ctx, adbc.ObjectDepthColumns, nil, nil, nil, nil, nil)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.Truef(adbc.GetObjectsSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", adbc.GetObjectsSchema, rdr.Schema())
	suite.True(rdr.Next())
	rec = rdr.Record()
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
		tableConstraintsList = dbSchemaTables.Field(3).(*array.List)
		tableConstraints     = tableConstraintsList.ListValues().(*array.Struct)

		colnames          = make([]string, 0)
		positions         = make([]string, 0)
		comments          = make([]string, 0)
		constraints       = make([]struct{ Name, Type string }, 0)
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
						xdbcDateTimeSub = append(xdbcDateTimeSub, strconv.Itoa(int(dtPrec)))

						charOctetLen := tableColumns.Field(12).(*array.Int32).Value(int(colIdx))
						xdbcCharOctetLen = append(xdbcCharOctetLen, strconv.Itoa(int(charOctetLen)))

						xdbcIsNullables = append(xdbcIsNullables, tableColumns.Field(13).(*array.String).Value(int(colIdx)))
					}

					conIdxStart, conIdxEnd := tableConstraintsList.ValueOffsets(int(tblIdx))
					for conIdx := conIdxStart; conIdx < conIdxEnd; conIdx++ {
						constraints = append(
							constraints,
							struct {
								Name string
								Type string
							}{
								Name: tableConstraints.Field(0).(*array.String).Value(int(conIdx)),
								Type: tableConstraints.Field(1).(*array.String).Value(int(conIdx)),
							})

					}
				}
			}
		}
	}

	suite.False(rdr.Next())
	suite.True(foundExpected)
	suite.ElementsMatch(expectedColnames, colnames)
	suite.ElementsMatch(expectedPositions, positions)
	suite.ElementsMatch(expectedComments, comments)
	suite.ElementsMatch(expectedConstraints, constraints)
	suite.ElementsMatch(expectedXdbcDataType, xdbcDataTypes)
	suite.ElementsMatch(expectedDataTypes, dataTypes)
	suite.ElementsMatch(expectedXdbcTypeName, xdbcTypeNames)
	suite.ElementsMatch(expectedXdbcCharMaxLen, xdbcCharMaxLens)
	suite.ElementsMatch(expectedXdbcScale, xdbcScales)
	suite.ElementsMatch(expectedXdbcNumPrecRadix, xdbcNumPrecRadixs)
	suite.ElementsMatch(expectedXdbcNullable, xdbcNullables)
	suite.ElementsMatch(expectedXdbcSqlDataType, xdbcSqlDataTypes)
	suite.ElementsMatch(expectedXdbcDateTimeSub, xdbcDateTimeSub)
	suite.ElementsMatch(expectedXdbcCharOctetLen, xdbcCharOctetLen)
	suite.ElementsMatch(expectedXdbcIsNullable, xdbcIsNullables)

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
	defer validation.CheckedClose(suite.T(), db)

	getSetDB, ok := db.(adbc.GetSetOptions)
	suite.True(ok)

	optVal1, err := getSetDB.GetOption(key1)
	suite.NoError(err)
	suite.Equal(optVal1, val1)
	optVal2, err := getSetDB.GetOption(key2)
	suite.NoError(err)
	suite.Equal(optVal2, val2)

	// set a new value for key1 and check that it was set
	newVal1 := "newval1"
	err = getSetDB.SetOption(key1, newVal1)
	suite.NoError(err)
	optVal1, err = getSetDB.GetOption(key1)
	suite.NoError(err)
	suite.Equal(optVal1, newVal1)
	// check that key2 is unchanged
	optVal2, err = getSetDB.GetOption(key2)
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

func (suite *SnowflakeTests) TestBooleanType() {
	suite.Require().NoError(suite.stmt.SetSqlQuery("select * from (SELECT CAST(TRUE  as BOOLEAN) as BOOLEANTYPE) as \"_\" where 0 = 1"))
	rdr, _, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	for _, f := range rdr.Schema().Fields() {
		st, ok := f.Metadata.GetValue("SNOWFLAKE_TYPE")
		if !ok {
			continue
		}
		if st == "boolean" {
			suite.Require().IsType(&arrow.BooleanType{}, f.Type)
		}
	}
}

func (suite *SnowflakeTests) TestTimestampPrecisionJson() {
	opts := suite.Quirks.DatabaseOptions()
	opts[driver.OptionMaxTimestampPrecision] = "microseconds"

	db, err := suite.driver.NewDatabase(opts)
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), db)
	cnxn, err := db.Open(suite.ctx)
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), cnxn)
	stmt, _ := cnxn.NewStatement()

	id := uuid.New()
	tempTable := "pk_" + strings.ReplaceAll(id.String(), "-", "")

	query := fmt.Sprintf(`CREATE OR REPLACE TABLE %s.%s.%s (
	id INT PRIMARY KEY,
	name STRING);`, suite.Quirks.catalogName, suite.Quirks.schemaName, tempTable)

	suite.Require().NoError(stmt.SetSqlQuery(query))
	_, err = stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.Require().NoError(stmt.Close())

	query = fmt.Sprintf("SHOW PRIMARY KEYS IN TABLE %s.%s.%s", suite.Quirks.catalogName, suite.Quirks.schemaName, tempTable)
	stmt, _ = cnxn.NewStatement()
	suite.Require().NoError(stmt.SetSqlQuery(query))
	rdr, _, err := stmt.ExecuteQuery(suite.ctx)
	defer rdr.Release()
	suite.Require().NoError(err)

	suite.True(rdr.Next())
	rec := rdr.Record()

	suite.Equal(1, int(rec.NumRows()))

	// Get column indexes
	getColIdx := func(name string) int {
		for i := 0; i < int(rec.NumCols()); i++ {
			if rec.ColumnName(i) == name {
				return i
			}
		}
		panic("Column not found: " + name)
	}

	// Expected column names
	dbIdx := getColIdx("database_name")
	schemaIdx := getColIdx("schema_name")
	tableIdx := getColIdx("table_name")
	colIdx := getColIdx("column_name")
	seqIdx := getColIdx("key_sequence")
	createdIdx := getColIdx("created_on")

	i := 0
	dbName := rec.Column(dbIdx).(*array.String).Value(i)
	schema := rec.Column(schemaIdx).(*array.String).Value(i)
	tbl := rec.Column(tableIdx).(*array.String).Value(i)
	column := rec.Column(colIdx).(*array.String).Value(i)
	keySeq := rec.Column(seqIdx).(*array.Int64).Value(i)

	// Created_on should be a timestamp array
	createdCol := rec.Column(createdIdx).(*array.Timestamp)
	created := time.Unix(0, int64(createdCol.Value(i))*int64(time.Microsecond)).UTC()

	// Perform checks
	if strings.EqualFold(dbName, suite.Quirks.catalogName) &&
		strings.EqualFold(schema, suite.Quirks.schemaName) &&
		strings.EqualFold(tbl, tempTable) &&
		strings.EqualFold(column, "id") &&
		keySeq == 1 {

		now := time.Now().UTC()
		diff := now.Sub(created)
		if diff < 0 {
			diff = -diff
		}
		// since this was just created, make sure the times are within a short difference
		suite.Assert().True(diff <= 2*time.Minute)
	} else {
		panic("Invalid values")
	}
	suite.Require().NoError(stmt.Close())

	query = fmt.Sprintf("DROP TABLE %s.%s.%s", suite.Quirks.catalogName, suite.Quirks.schemaName, tempTable)
	stmt, _ = cnxn.NewStatement()
	suite.Require().NoError(stmt.SetSqlQuery(query))
	_, err = stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.Require().NoError(stmt.Close())
}

func (suite *SnowflakeTests) TestTimestampPrecision() {

	query_ntz := "select TO_TIMESTAMP_NTZ('0001-01-01 00:00:00.000000000') as Jan01_0001, TO_TIMESTAMP_NTZ('2025-06-02 10:37:56.123456789') as June02_2025, TO_TIMESTAMP_NTZ('9999-12-31 23:59:59.999999999') As December31_9999"
	query_ltz := "select TO_TIMESTAMP_LTZ('0001-01-01 00:00:00.000000000') as Jan01_0001, TO_TIMESTAMP_LTZ('2025-06-02 10:37:56.123456789') as June02_2025, TO_TIMESTAMP_LTZ('9999-12-31 23:59:59.999999999') As December31_9999"
	query_tz := "select TO_TIMESTAMP_TZ('0001-01-01 00:00:00.000000000') as Jan01_0001, TO_TIMESTAMP_TZ('2025-06-02 10:37:56.123456789') as June02_2025, TO_TIMESTAMP_TZ('9999-12-31 23:59:59.999999999') As December31_9999"

	v1, _ := arrow.TimestampFromTime(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), arrow.Microsecond)
	v2, _ := arrow.TimestampFromTime(time.Date(2025, 6, 2, 10, 37, 56, 123456789, time.UTC), arrow.Microsecond)
	v3, _ := arrow.TimestampFromTime(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC), arrow.Microsecond)

	// Expected values
	expectedMicrosecondsResults := []arrow.Timestamp{
		v1,
		v2,
		v3,
	}

	expectedNanosecondResults := []arrow.Timestamp{
		//overflows to August 30, 1754 at 22:43:41.128654 UTC.
		arrow.Timestamp(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()),
		arrow.Timestamp(time.Date(2025, 6, 2, 10, 37, 56, 123456789, time.UTC).UnixNano()),
		//overflows to March 30, 1816 at 05:56:08.066278 UTC.
		arrow.Timestamp(time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC).UnixNano()),
	}

	suite.queryTimestamps(query_ntz, expectedMicrosecondsResults, expectedNanosecondResults)
	suite.queryTimestamps(query_ltz, expectedMicrosecondsResults, expectedNanosecondResults)
	suite.queryTimestamps(query_tz, expectedMicrosecondsResults, expectedNanosecondResults)
}

func (suite *SnowflakeTests) queryTimestamps(query string, expectedMicrosecondResults []arrow.Timestamp, expectedNanosecondResults []arrow.Timestamp) {

	// with max microseconds precision
	rec := suite.getTimestamps(query, driver.OptionValueMicroseconds)
	suite.validateTimestamps(query, rec, expectedMicrosecondResults)

	// with the default nanoseconds precision
	rec = suite.getTimestamps(query, driver.OptionValueNanoseconds)
	suite.validateTimestamps(query, rec, expectedNanosecondResults)

	// set the strict option to error on overflow
	rec = suite.getTimestamps(query, driver.OptionValueNanosecondsNoOverflow)
	suite.validateTimestamps(query, rec, nil) // dont expect any results
}

func (suite *SnowflakeTests) getTimestamps(query string, maxTimestampPrecision string) arrow.Record {

	// with max microseconds precision
	opts := suite.Quirks.DatabaseOptions()
	if maxTimestampPrecision != "" {
		opts[driver.OptionMaxTimestampPrecision] = maxTimestampPrecision
	}
	db, err := suite.driver.NewDatabase(opts)
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), db)
	cnxn, err := db.Open(suite.ctx)
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), cnxn)
	stmt, _ := cnxn.NewStatement()
	suite.Require().NoError(stmt.SetSqlQuery(query))
	rdr, _, err := stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)

	defer rdr.Release()

	if maxTimestampPrecision == driver.OptionValueNanosecondsNoOverflow {
		suite.False(rdr.Next())
		return nil
	}

	suite.True(rdr.Next())
	rec := rdr.Record()

	return rec
}

func (suite *SnowflakeTests) validateTimestamps(query string, rec arrow.Record, expected []arrow.Timestamp) {
	if expected != nil {
		for i := 0; i < int(rec.NumCols()); i++ {
			col := rec.Column(i).(*array.Timestamp)
			suite.EqualValues(1, col.Len())
			actual := col.Value(0)
			suite.Equal(expected[i], actual, "Mismatch in column %d for the query %d", i, query)
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

			suite.False(rdr.Next())
		}
	}
}

func (suite *SnowflakeTests) TestSchemaWithLowPrecision() {
	// This test verifies that the schema type matches the actual data type
	// for NUMBER columns when use_high_precision=false for scale=0 and scale>0
	// This is critical for clients that rely on the schema being correct
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "SCHEMA_TYPE_TEST"))

	suite.Require().NoError(suite.stmt.SetSqlQuery(`CREATE OR REPLACE TABLE SCHEMA_TYPE_TEST (
		INTEGER_COL NUMBER(10,0),
		DECIMAL_COL NUMBER(10,2)
	)`))
	_, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(`INSERT INTO SCHEMA_TYPE_TEST VALUES
		(12345, 123.45),
		(67890, 678.90)`))
	_, err = suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

	// Test with use_high_precision=false
	suite.Require().NoError(suite.stmt.SetOption(driver.OptionUseHighPrecision, adbc.OptionValueDisabled))
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT * FROM SCHEMA_TYPE_TEST"))

	// First test: ExecuteSchema (schema only, no data)
	schemaOnly, err := suite.stmt.(adbc.StatementExecuteSchema).ExecuteSchema(suite.ctx)
	suite.Require().NoError(err)

	// Verify ExecuteSchema returns correct types
	suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Int64, schemaOnly.Field(0).Type),
		"ExecuteSchema INTEGER_COL: expected int64, got %s", schemaOnly.Field(0).Type)
	suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Float64, schemaOnly.Field(1).Type),
		"ExecuteSchema DECIMAL_COL: expected float64, got %s", schemaOnly.Field(1).Type)

	// Second test: ExecuteQuery (schema with data)
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(2, n)

	// Check schema types from ExecuteQuery
	schema := rdr.Schema()
	suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Int64, schema.Field(0).Type),
		"INTEGER_COL: expected int64 in schema, got %s", schema.Field(0).Type)
	suite.Truef(arrow.TypeEqual(arrow.PrimitiveTypes.Float64, schema.Field(1).Type),
		"DECIMAL_COL: expected float64 in schema, got %s", schema.Field(1).Type)

	// Check actual data types in the record
	suite.True(rdr.Next())
	rec := rdr.Record()

	// Verify INTEGER_COL is actually Int64
	col0 := rec.Column(0)
	suite.Equal(arrow.INT64, col0.DataType().ID(),
		"INTEGER_COL data: expected INT64 type, got %s", col0.DataType())
	suite.Equal(int64(12345), col0.(*array.Int64).Value(0))

	// Verify DECIMAL_COL is actually Float64
	col1 := rec.Column(1)
	suite.Equal(arrow.FLOAT64, col1.DataType().ID(),
		"DECIMAL_COL data: expected FLOAT64 type, got %s", col1.DataType())
	suite.InDelta(123.45, col1.(*array.Float64).Value(0), 0.001)

	// Check second row
	suite.Equal(int64(67890), col0.(*array.Int64).Value(1))
	suite.InDelta(678.90, col1.(*array.Float64).Value(1), 0.001)

	// Verify schema type matches actual data type
	suite.Equal(schema.Field(1).Type.ID(), col1.DataType().ID(),
		"Schema type must match data type for DECIMAL_COL: schema says %s, data is %s",
		schema.Field(1).Type, col1.DataType())
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
	defer rdr.Release()

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

	ConnectWithJwt(t, uri, keyValue, "")
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

	ConnectWithJwt(t, uri, keyValue, passcode)
}

func ConnectWithJwt(t *testing.T, uri, keyValue, passcode string) {

	// Windows funkiness
	if runtime.GOOS == "windows" {
		keyValue = strings.ReplaceAll(keyValue, "\\r", "\r")
		keyValue = strings.ReplaceAll(keyValue, "\\n", "\n")
	}

	cfg, err := gosnowflake.ParseDSN(uri)
	assert.NoError(t, err)

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
	assert.NoError(t, err)
	defer validation.CheckedClose(t, db)

	cnxn, err := db.Open(context.Background())
	assert.NoError(t, err)
	defer validation.CheckedClose(t, cnxn)
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
		defer validation.CheckedClose(suite.T(), db)
		cnxn, err := db.Open(suite.ctx)
		suite.NoError(err)
		defer validation.CheckedClose(suite.T(), cnxn)
		stmt, err := cnxn.NewStatement()
		suite.NoError(err)
		defer validation.CheckedClose(suite.T(), stmt)

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
	defer validation.CheckedCleanup(suite.T(), func() error {
		return os.Remove(pkcs1Key)
	})
	verifyKey(pkcs1Key)

	// PKCS8 key
	rsaKeyP8Bytes, _ := x509.MarshalPKCS8PrivateKey(rsaKey)
	rsaKeyP8 := pem.EncodeToMemory(&pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: rsaKeyP8Bytes,
	})
	pkcs8Key := writeKey("key.p8", rsaKeyP8)
	defer validation.CheckedCleanup(suite.T(), func() error {
		return os.Remove(pkcs8Key)
	})
	verifyKey(pkcs8Key)

	// binary key
	block, _ := pem.Decode([]byte(rsaKeyPem))
	binKey := writeKey("key.bin", block.Bytes)
	defer validation.CheckedCleanup(suite.T(), func() error {
		return os.Remove(binKey)
	})
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

	// verify that we got the expected number of rows if we sum up
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

	// verify that we got the expected number of rows if we sum up
	// all the rows from each record in the stream.
	suite.Equal(n, recv)
	suite.Equal(recv, int64(0))
}

func (suite *SnowflakeTests) TestIngestEmptyChunk() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest_empty_chunk"))

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
	defer bldr.Release()

	emptyRec := bldr.NewRecord()
	defer emptyRec.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	// See https://github.com/apache/arrow-adbc/issues/1847
	// Snowflake does not properly handle empty row groups, so need to make sure we don't send any.
	rdr, err := array.NewRecordReader(sc, []arrow.Record{emptyRec, rec})
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.Require().NoError(suite.stmt.BindStream(suite.ctx, rdr))
	suite.Require().NoError(suite.stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_empty_chunk"))
	suite.Require().NoError(suite.stmt.SetOption(driver.OptionStatementIngestWriterConcurrency, "1"))

	n, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.EqualValues(int64(3), n)
}

func TestIngestCancelContext(t *testing.T) {
	withQuirks(t, func(q *SnowflakeQuirks) {
		ctx := context.Background()

		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabase(q.DatabaseOptions())
		require.NoError(t, err)

		cnxn, err := db.Open(ctx)
		require.NoError(t, err)

		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)

		require.NoError(t, q.DropTable(cnxn, "bulk_ingest_cancel_context"))

		sc := arrow.NewSchema([]arrow.Field{
			{
				Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
				Nullable: true,
			},
		}, nil)

		bldr := array.NewRecordBuilder(q.Alloc(), sc)
		defer bldr.Release()

		bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

		rec := bldr.NewRecord()
		defer rec.Release()

		rdr, err := array.NewRecordReader(sc, []arrow.Record{rec})
		require.NoError(t, err)
		defer rdr.Release()

		require.NoError(t, stmt.BindStream(ctx, rdr))
		require.NoError(t, stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest_cancel_context"))

		var buf bytes.Buffer
		logger := gosnowflake.GetLogger()
		logger.SetOutput(&buf)
		defer logger.SetOutput(os.Stderr)

		ctxCancel, cancel := context.WithCancel(ctx)
		n, err := stmt.ExecuteUpdate(ctxCancel)
		require.NoError(t, err)
		require.EqualValues(t, int64(3), n)
		cancel()

		require.NoError(t, stmt.Close())
		require.NoError(t, cnxn.Close())
		require.NoError(t, db.Close())

		require.Equal(t, "", buf.String())
	})
}

func (suite *SnowflakeTests) TestChangeDatabaseAndGetObjects() {
	// this test demonstrates:
	// 1. changing the database context
	// 2. being able to call GetObjects after changing the database context
	//    (this uses a different connection context but still executes successfully)

	uri, ok := os.LookupEnv("SNOWFLAKE_URI")
	if !ok {
		suite.T().Skip("Cannot find the `SNOWFLAKE_URI` value")
	}

	newCatalog, ok := os.LookupEnv("SNOWFLAKE_NEW_CATALOG")
	if !ok {
		suite.T().Skip("Cannot find the `SNOWFLAKE_NEW_CATALOG` value")
	}

	getObjectsTable, ok := os.LookupEnv("SNOWFLAKE_TABLE_GETOBJECTS")
	if !ok {
		suite.T().Skip("Cannot find the `SNOWFLAKE_TABLE_GETOBJECTS` value")
	}

	cfg, err := gosnowflake.ParseDSN(uri)
	suite.NoError(err)

	cnxnopt, ok := suite.cnxn.(adbc.PostInitOptions)
	suite.True(ok)
	err = cnxnopt.SetOption(adbc.OptionKeyCurrentCatalog, newCatalog)
	suite.NoError(err)

	_, err2 := suite.cnxn.GetObjects(suite.ctx, adbc.ObjectDepthAll, &newCatalog, &cfg.Schema, &getObjectsTable, nil, nil)
	suite.NoError(err2)
}

func (suite *SnowflakeTests) TestGetSetClientConfigFile() {
	file := "fileNameJustForTest.json"
	options := map[string]string{
		driver.OptionClientConfigFile: file,
	}
	getSetDB, ok := suite.db.(adbc.GetSetOptions)
	suite.True(ok)
	err := suite.db.SetOptions(options)
	suite.NoError(err)
	result, err := getSetDB.GetOption(driver.OptionClientConfigFile)
	suite.NoError(err)
	suite.True(file == result)
}

func (suite *SnowflakeTests) TestGetObjectsWithNilCatalog() {
	// this test demonstrates calling GetObjects with the catalog depth and a nil catalog
	rdr, err := suite.cnxn.GetObjects(suite.ctx, adbc.ObjectDepthCatalogs, nil, nil, nil, nil, nil)
	suite.NoError(err)
	// test suite validates memory allocator so we need to make sure we call
	// release on the result reader
	rdr.Release()
}

func TestJSONUnmarshal(t *testing.T) {
	jsonData := `{
  "catalog_db_schemas": [
    {
      "db_schema_name": "PUBLIC",
      "db_schema_tables": [
        {
          "table_columns": [
            {
              "column_name": "PRODUCT_ID",
              "ordinal_position": 1,
              "xdbc_char_octet_length": 16777216,
              "xdbc_column_size": 16777216,
              "xdbc_is_nullable": "NO",
              "xdbc_nullable": 0,
              "xdbc_type_name": "TEXT"
            },
            {
              "column_name": "PRODUCT_NAME",
              "ordinal_position": 2,
              "xdbc_char_octet_length": 16777216,
              "xdbc_column_size": 16777216,
              "xdbc_is_nullable": "YES",
              "xdbc_nullable": 1,
              "xdbc_type_name": "TEXT"
            },
            {
              "column_name": "ORDER_ID",
              "ordinal_position": 3,
              "xdbc_char_octet_length": 16777216,
              "xdbc_column_size": 16777216,
              "xdbc_is_nullable": "YES",
              "xdbc_nullable": 1,
              "xdbc_type_name": "TEXT"
            }
          ],
          "table_constraints": [
            {
              "constraint_column_names": [
                "PRODUCT_ID"
              ],
              "constraint_column_usage": [],
              "constraint_name": "SYS_CONSTRAINT_386a9022-ad6e-47ed-94b6-f48501938f5f",
              "constraint_type": "PRIMARY KEY"
            },
            {
              "constraint_column_names": [
                "ORDER_ID"
              ],
              "constraint_column_usage": [
                {
                  "fk_catalog": "DEMODB",
                  "fk_column_name": "ORDER_ID",
                  "fk_db_schema": "PUBLIC",
                  "fk_table": "ORDERS2"
                }
              ],
              "constraint_name": "SYS_CONSTRAINT_cfb3b763-4a97-4c0b-8356-a493c03b7e66",
              "constraint_type": "FOREIGN KEY"
            }
          ],
          "table_name": "PRODUCT2",
          "table_type": "BASE TABLE"
        }
      ]
    }
  ],
  "catalog_name": "DEMODB"
}`

	var getObjectsCatalog driverbase.GetObjectsInfo
	require.NoError(t, json.Unmarshal([]byte(jsonData), &getObjectsCatalog))

	for _, sch := range getObjectsCatalog.CatalogDbSchemas {
		for _, tab := range sch.DbSchemaTables {
			for _, con := range tab.TableConstraints {
				assert.NotEmpty(t, con.ConstraintColumnNames)
			}
		}
	}
}

func (suite *SnowflakeTests) TestQueryTag() {
	u, err := uuid.NewV7()
	suite.Require().NoError(err)
	tag := u.String()
	suite.Require().NoError(suite.stmt.SetOption(driver.OptionStatementQueryTag, tag))

	val, err := suite.stmt.(adbc.GetSetOptions).GetOption(driver.OptionStatementQueryTag)
	suite.Require().NoError(err)
	suite.Require().Equal(tag, val)

	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT 1"))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(1, n)
	suite.True(rdr.Next())
	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())

	// Unset tag
	suite.Require().NoError(suite.stmt.SetOption(driver.OptionStatementQueryTag, ""))

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf(`
SELECT query_text
FROM table(information_schema.query_history())
WHERE query_tag = '%s'
ORDER BY start_time;
`, tag)))
	rdr, n, err = suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(1, n)
	suite.True(rdr.Next())
	result := rdr.Record()
	suite.Require().Equal("SELECT 1", result.Column(0).(*array.String).Value(0))
	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *SnowflakeTests) TestGetObjectsVector() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "MYVECTORTABLE"))
	suite.Require().NoError(suite.stmt.SetSqlQuery(`CREATE OR REPLACE TABLE myvectortable (
		a VECTOR(float, 3), b VECTOR(float, 3))`))
	_, err := suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)
	suite.Require().NoError(suite.stmt.SetSqlQuery(`INSERT INTO myvectortable
		SELECT [1.1,2.2,3]::VECTOR(FLOAT,3), [1,1,1]::VECTOR(FLOAT,3)`))
	_, err = suite.stmt.ExecuteUpdate(suite.ctx)
	suite.Require().NoError(err)

	tableName := "MYVECTORTABLE"
	rdr, err := suite.cnxn.GetObjects(suite.ctx, adbc.ObjectDepthColumns, nil, nil, &tableName, nil, nil)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.Require().True(rdr.Next())
	rec := rdr.Record()

	for i := 0; int64(i) < rec.NumRows(); i++ {
		// list<db_schema_schema>
		dbSchemasList := rec.Column(1).(*array.List)
		// db_schema_schema (struct)
		dbSchemas := dbSchemasList.ListValues().(*array.Struct)
		// list<table_schema>
		dbSchemaTablesList := dbSchemas.Field(1).(*array.List)
		// table_schema (struct)
		dbSchemaTables := dbSchemaTablesList.ListValues().(*array.Struct)
		// list<column_schema>
		tableColumnsList := dbSchemaTables.Field(2).(*array.List)
		// column_schema (struct)
		tableColumns := tableColumnsList.ListValues().(*array.Struct)

		start, end := dbSchemasList.ValueOffsets(i)
		for j := start; j < end; j++ {
			schemaName := dbSchemas.Field(0).(*array.String).Value(int(j))
			if !strings.EqualFold(schemaName, suite.Quirks.DBSchema()) {
				continue
			}
			tblStart, tblEnd := dbSchemaTablesList.ValueOffsets(int(j))
			for k := tblStart; k < tblEnd; k++ {
				tblName := dbSchemaTables.Field(0).(*array.String).Value(int(k))
				if !strings.EqualFold(tblName, tableName) {
					continue
				}

				colStart, colEnd := tableColumnsList.ValueOffsets(int(k))
				suite.EqualValues(2, colEnd-colStart)

				for l := colStart; l < colEnd; l++ {
					colName := tableColumns.Field(0).(*array.String).Value(int(l))
					ordinalPos := tableColumns.Field(1).(*array.Int32).Value(int(l))
					typeName := tableColumns.Field(4).(*array.String).Value(int(l))
					switch ordinalPos {
					case 1:
						suite.Equal("A", colName)
					case 2:
						suite.Equal("B", colName)
					}

					suite.Equal("VECTOR", typeName)
				}
			}
		}
	}
}
