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
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/snowflake"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/google/uuid"
	"github.com/snowflakedb/gosnowflake"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type SnowflakeQuirks struct {
	dsn        string
	mem        *memory.CheckedAllocator
	connector  gosnowflake.Connector
	schemaName string
}

func (s *SnowflakeQuirks) SetupDriver(t *testing.T) adbc.Driver {
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	cfg, err := gosnowflake.ParseDSN(s.dsn)
	require.NoError(t, err)

	cfg.Schema = s.schemaName
	s.connector = gosnowflake.NewConnector(gosnowflake.SnowflakeDriver{}, *cfg)
	return driver.Driver{Alloc: s.mem}
}

func (s *SnowflakeQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	s.mem.AssertSize(t, 0)
}

func (s *SnowflakeQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI:   s.dsn,
		driver.OptionSchema: s.schemaName,
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
func (s *SnowflakeQuirks) SupportsConcurrentStatements() bool    { return true }
func (s *SnowflakeQuirks) SupportsPartitionedData() bool         { return false }
func (s *SnowflakeQuirks) SupportsTransactions() bool            { return true }
func (s *SnowflakeQuirks) SupportsGetParameterSchema() bool      { return false }
func (s *SnowflakeQuirks) SupportsDynamicParameterBinding() bool { return false }
func (s *SnowflakeQuirks) SupportsBulkIngest() bool              { return true }
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

func createTempSchema(uri string) string {
	db, err := sql.Open("snowflake", uri)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	schemaName := "ADBC_TESTING_" + strings.ReplaceAll(uuid.New().String(), "-", "_")
	_, err = db.Exec(`CREATE SCHEMA ADBC_TESTING.` + schemaName)
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

	if uri == "" {
		t.Skip("no SNOWFLAKE_URI defined, skip snowflake driver tests")
	}

	// avoid multiple runs clashing by operating in a fresh schema and then
	// dropping that schema when we're done.
	q := &SnowflakeQuirks{dsn: uri, schemaName: createTempSchema(uri)}
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
	suite.db = nil
}

func (suite *SnowflakeTests) TestSqlIngestTimestamp() {
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest"))

	sc := arrow.NewSchema([]arrow.Field{{
		Name: "col", Type: arrow.FixedWidthTypes.Timestamp_us,
		Nullable: true,
	}}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, sc)
	defer bldr.Release()

	tbldr := bldr.Field(0).(*array.TimestampBuilder)
	tbldr.AppendValues([]arrow.Timestamp{0, 0, 42}, []bool{false, true, true})
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
	_, _, err := suite.stmt.ExecuteQuery(suite.ctx)
	var adbcErr adbc.Error
	suite.ErrorAs(err, &adbcErr)

	suite.Equal(adbc.StatusInternal, adbcErr.Code)
	suite.Contains(adbcErr.Msg, "Cannot get Arrow data from this result set")
}
