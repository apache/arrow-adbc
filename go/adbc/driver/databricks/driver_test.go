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

package databricks_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type DatabricksQuirks struct {
	mem         *memory.CheckedAllocator
	catalogName string
	schemaName  string
	hostname    string
	httpPath    string
	token       string
	port        string
	uri         string // The URI to use for the test if set
}

func (d *DatabricksQuirks) SetupDriver(t *testing.T) adbc.Driver {
	d.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return databricks.NewDriver(d.mem)
}

func (d *DatabricksQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	d.mem.AssertSize(t, 0)
}

func (d *DatabricksQuirks) DatabaseOptions() map[string]string {
	if d.uri != "" {
		return map[string]string{
			adbc.OptionKeyURI: d.uri,
		}
	}

	opts := map[string]string{
		databricks.OptionServerHostname: d.hostname,
		databricks.OptionHTTPPath:       d.httpPath,
		databricks.OptionAccessToken:    d.token,
	}

	if d.port != "" {
		opts[databricks.OptionPort] = d.port
	}
	if d.catalogName != "" {
		opts[databricks.OptionCatalog] = d.catalogName
	}
	if d.schemaName != "" {
		opts[databricks.OptionSchema] = d.schemaName
	}

	return opts
}

func (d *DatabricksQuirks) getSqlTypeFromArrowType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT32:
		return "FLOAT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.STRING:
		return "STRING"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.DATE32:
		return "DATE"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.BINARY:
		return "BINARY"
	default:
		return "STRING"
	}
}

func quoteTblName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func (d *DatabricksQuirks) CreateSampleTable(tableName string, r arrow.RecordBatch) error {
	drv := databricks.NewDriver(d.mem)
	db, err := drv.NewDatabase(d.DatabaseOptions())
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	cnxn, err := db.Open(context.Background())
	if err != nil {
		return err
	}
	defer func() { _ = cnxn.Close() }()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	var b strings.Builder
	b.WriteString("CREATE OR REPLACE TABLE ")
	b.WriteString(quoteTblName(tableName))
	b.WriteString(" (")

	for i := 0; i < int(r.NumCols()); i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		f := r.Schema().Field(i)
		b.WriteString(quoteTblName(f.Name))
		b.WriteByte(' ')
		b.WriteString(d.getSqlTypeFromArrowType(f.Type))
	}

	b.WriteString(")")

	if err := stmt.SetSqlQuery(b.String()); err != nil {
		return err
	}
	if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
		return err
	}

	if r.NumRows() == 0 {
		return nil
	}

	return d.insertDataRows(stmt, tableName, r)
}

func (d *DatabricksQuirks) insertDataRows(stmt adbc.Statement, tableName string, r arrow.RecordBatch) error {
	if r.NumRows() == 0 {
		return nil
	}

	for row := 0; row < int(r.NumRows()); row++ {
		var values []string
		for col := 0; col < int(r.NumCols()); col++ {
			column := r.Column(col)
			if column.IsNull(row) {
				values = append(values, "NULL")
			} else {
				values = append(values, d.getSimpleTestValue(column.DataType(), row))
			}
		}

		querySQL := fmt.Sprintf("INSERT INTO %s VALUES (%s)",
			quoteTblName(tableName), strings.Join(values, ", "))

		if err := stmt.SetSqlQuery(querySQL); err != nil {
			return err
		}
		if _, err := stmt.ExecuteUpdate(context.Background()); err != nil {
			return err
		}
	}
	return nil
}

func (d *DatabricksQuirks) getSimpleTestValue(dataType arrow.DataType, row int) string {
	switch dataType.ID() {
	case arrow.INT8, arrow.UINT8, arrow.INT16, arrow.UINT16, arrow.INT32, arrow.UINT32, arrow.INT64, arrow.UINT64:
		return fmt.Sprintf("%d", row+1)
	case arrow.FLOAT32, arrow.FLOAT64:
		return fmt.Sprintf("%g", float64(row)+0.5)
	case arrow.STRING:
		return fmt.Sprintf("'test_string_%d'", row)
	case arrow.BINARY:
		return fmt.Sprintf("X'%02X'", row)
	case arrow.BOOL:
		if row%2 == 0 {
			return "TRUE"
		}
		return "FALSE"
	case arrow.DATE32:
		return "'2023-01-01'"
	case arrow.TIMESTAMP:
		return "'2023-01-01 12:00:00.000000'"
	case arrow.DECIMAL128:
		return fmt.Sprintf("%d.50", row+1)
	default:
		return fmt.Sprintf("'test_value_%d'", row)
	}
}

func (d *DatabricksQuirks) DropTable(cnxn adbc.Connection, tblname string) error {
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

func (d *DatabricksQuirks) Alloc() memory.Allocator                     { return d.mem }
func (d *DatabricksQuirks) BindParameter(index int) string              { return "?" }
func (d *DatabricksQuirks) SupportsBulkIngest(mode string) bool         { return true }
func (d *DatabricksQuirks) SupportsConcurrentStatements() bool          { return true }
func (d *DatabricksQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (d *DatabricksQuirks) SupportsExecuteSchema() bool                 { return true }
func (d *DatabricksQuirks) SupportsGetSetOptions() bool                 { return true }
func (d *DatabricksQuirks) SupportsPartitionedData() bool               { return false }
func (d *DatabricksQuirks) SupportsStatistics() bool                    { return false }
func (d *DatabricksQuirks) SupportsTransactions() bool                  { return true }
func (d *DatabricksQuirks) SupportsGetParameterSchema() bool            { return false }
func (d *DatabricksQuirks) SupportsDynamicParameterBinding() bool       { return false }
func (d *DatabricksQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (d *DatabricksQuirks) Catalog() string                             { return d.catalogName }
func (d *DatabricksQuirks) DBSchema() string                            { return d.schemaName }

func (d *DatabricksQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Databricks Driver - Go"
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
		return "Databricks"
	}
	return nil
}

func (d *DatabricksQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	switch dt.ID() {
	case arrow.STRING:
		return arrow.MetadataFrom(map[string]string{
			"DATA_TYPE": "STRING", "PRIMARY_KEY": "N",
		})
	case arrow.INT64:
		return arrow.MetadataFrom(map[string]string{
			"DATA_TYPE": "BIGINT", "PRIMARY_KEY": "N",
		})
	case arrow.FLOAT64:
		return arrow.MetadataFrom(map[string]string{
			"DATA_TYPE": "DOUBLE", "PRIMARY_KEY": "N",
		})
	case arrow.BOOL:
		return arrow.MetadataFrom(map[string]string{
			"DATA_TYPE": "BOOLEAN", "PRIMARY_KEY": "N",
		})
	}
	return arrow.Metadata{}
}

func (suite *DatabricksTests) checkRowCount(expected int64, actual int64) {
	if actual != -1 {
		suite.EqualValues(expected, actual)
	}
}

func withQuirks(t *testing.T, fn func(*DatabricksQuirks)) {
	hostname := os.Getenv("DATABRICKS_HOST")
	httpPath := os.Getenv("DATABRICKS_HTTPPATH")
	token := os.Getenv("DATABRICKS_ACCESSTOKEN")
	catalog := os.Getenv("DATABRICKS_CATALOG")
	schema := os.Getenv("DATABRICKS_SCHEMA")

	if hostname == "" {
		t.Skip("DATABRICKS_HOST not defined, skipping Databricks driver tests")
	} else if httpPath == "" {
		t.Skip("DATABRICKS_HTTPPATH not defined, skipping Databricks driver tests")
	} else if token == "" {
		t.Skip("DATABRICKS_ACCESSTOKEN not defined, skipping Databricks driver tests")
	}

	if catalog == "" {
		catalog = "main"
	}
	if schema == "" {
		schema = "default"
	}
	q := &DatabricksQuirks{
		hostname:    hostname,
		httpPath:    httpPath,
		token:       token,
		catalogName: catalog,
		schemaName:  schema,
		port:        os.Getenv("DATABRICKS_PORT"), // optional
	}

	fn(q)
}

func withQuirksURI(t *testing.T, fn func(*DatabricksQuirks)) {
	uri := os.Getenv("DATABRICKS_URI")
	if uri == "" {
		t.Skip("DATABRICKS_URI not defined, skipping URI tests")
	}

	q := &DatabricksQuirks{
		uri: uri,
	}
	fn(q)
}

func TestValidation(t *testing.T) {
	withQuirks(t, func(q *DatabricksQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

func TestDatabricks(t *testing.T) {
	withQuirks(t, func(q *DatabricksQuirks) {
		suite.Run(t, &DatabricksTests{Quirks: q})
	})
}

func TestDatabricksWithURI(t *testing.T) {
	withQuirksURI(t, func(q *DatabricksQuirks) {
		drv := q.SetupDriver(t)
		defer q.TearDownDriver(t, drv)

		db, err := drv.NewDatabase(q.DatabaseOptions())
		require.NoError(t, err)
		defer validation.CheckedClose(t, db)

		ctx := context.Background()
		cnxn, err := db.Open(ctx)
		require.NoError(t, err)
		defer validation.CheckedClose(t, cnxn)

		stmt, err := cnxn.NewStatement()
		require.NoError(t, err)
		defer validation.CheckedClose(t, stmt)

		require.NoError(t, stmt.SetSqlQuery("SELECT 1 as test_col"))
		rdr, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer rdr.Release()

		assert.True(t, rdr.Next())
		rec := rdr.RecordBatch()
		assert.Equal(t, int64(1), rec.NumRows())
		assert.Equal(t, int64(1), rec.NumCols())
		assert.Equal(t, "test_col", rec.ColumnName(0))
		assert.False(t, rdr.Next())
		require.NoError(t, rdr.Err())
	})
}

// ---- Additional Tests --------------------

type DatabricksTests struct {
	suite.Suite

	Quirks *DatabricksQuirks

	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (suite *DatabricksTests) SetupTest() {
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

func (suite *DatabricksTests) TearDownTest() {
	validation.CheckedClose(suite.T(), suite.stmt)
	validation.CheckedClose(suite.T(), suite.cnxn)
	validation.CheckedClose(suite.T(), suite.db)
	suite.Quirks.TearDownDriver(suite.T(), suite.driver)
	suite.cnxn = nil
	suite.db = nil
	suite.driver = nil
}

func (suite *DatabricksTests) TestNewDatabaseWithOptions() {
	t := suite.T()

	drv := suite.Quirks.SetupDriver(t)

	t.Run("WithBasicOptions", func(t *testing.T) {
		dbOptions := suite.Quirks.DatabaseOptions()
		db, err := drv.NewDatabase(dbOptions)
		suite.NoError(err)
		suite.NotNil(db)
		cnxn, err := db.Open(suite.ctx)
		suite.NoError(err)
		suite.NotNil(cnxn)
		defer validation.CheckedClose(suite.T(), cnxn)
		defer validation.CheckedClose(suite.T(), db)
	})

	t.Run("WithPort", func(t *testing.T) {
		dbOptions := suite.Quirks.DatabaseOptions()
		dbOptions[databricks.OptionPort] = "443"
		db, err := drv.NewDatabase(dbOptions)
		suite.NoError(err)
		suite.NotNil(db)
		defer validation.CheckedClose(suite.T(), db)
	})

	t.Run("WithSSLOptions", func(t *testing.T) {
		dbOptions := suite.Quirks.DatabaseOptions()
		dbOptions[databricks.OptionSSLMode] = "require"
		db, err := drv.NewDatabase(dbOptions)
		suite.NoError(err)
		suite.NotNil(db)
		defer validation.CheckedClose(suite.T(), db)
	})
}

func (suite *DatabricksTests) TestConnectionOptions() {
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT 1 as test_col"))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	_ = n
	suite.True(rdr.Next())
	rec := rdr.RecordBatch()
	suite.Equal(int64(1), rec.NumRows())
	suite.Equal(int64(1), rec.NumCols())
	suite.Equal("test_col", rec.ColumnName(0))
	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *DatabricksTests) TestBasicDataTypes() {
	suite.Require().NoError(suite.stmt.SetSqlQuery(`
		SELECT
			CAST(42 AS BIGINT) as bigint_col,
			CAST(3.14 AS DOUBLE) as double_col,
			CAST('hello' AS STRING) as string_col,
			CAST(true AS BOOLEAN) as boolean_col
	`))

	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.checkRowCount(1, n)
	suite.True(rdr.Next())
	rec := rdr.RecordBatch()

	suite.Equal(int64(4), rec.NumCols())
	suite.Equal("bigint_col", rec.ColumnName(0))
	suite.Equal("double_col", rec.ColumnName(1))
	suite.Equal("string_col", rec.ColumnName(2))
	suite.Equal("boolean_col", rec.ColumnName(3))

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *DatabricksTests) TestStatementEmptyResultSet() {
	suite.NoError(suite.stmt.SetSqlQuery("SELECT 1 WHERE 1=0"))

	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.checkRowCount(0, n)
	suite.False(rdr.Next())
	suite.NoError(rdr.Err())
}

func (suite *DatabricksTests) TestGetSetOptions() {
	getSetDB, ok := suite.db.(adbc.GetSetOptions)
	suite.True(ok, "Database should implement GetSetOptions")

	testKey := databricks.OptionQueryTimeout
	testValue := "1m0s"

	err := getSetDB.SetOption(testKey, testValue)
	suite.NoError(err)

	retrievedValue, err := getSetDB.GetOption(testKey)
	suite.NoError(err)
	suite.Equal(testValue, retrievedValue)
}

func (suite *DatabricksTests) TestQueryTimeout() {
	dbOptions := suite.Quirks.DatabaseOptions()
	dbOptions[databricks.OptionQueryTimeout] = "30s"

	db, err := suite.driver.NewDatabase(dbOptions)
	suite.NoError(err)

	cnxn, err := db.Open(suite.ctx)
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), cnxn)
	defer validation.CheckedClose(suite.T(), db)

	stmt, err := cnxn.NewStatement()
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	suite.Require().NoError(stmt.SetSqlQuery("SELECT 1"))
	rdr, _, err := stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.True(rdr.Next())
	suite.False(rdr.Next())
}

func (suite *DatabricksTests) TestMaxRows() {
	dbOptions := suite.Quirks.DatabaseOptions()
	dbOptions[databricks.OptionMaxRows] = "100"

	db, err := suite.driver.NewDatabase(dbOptions)
	suite.NoError(err)

	cnxn, err := db.Open(suite.ctx)
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), cnxn)
	defer validation.CheckedClose(suite.T(), db)

	stmt, err := cnxn.NewStatement()
	suite.NoError(err)
	defer validation.CheckedClose(suite.T(), stmt)

	// Generate a query that would return more than 100 rows if not limited
	suite.Require().NoError(stmt.SetSqlQuery("SELECT id FROM range(200)"))
	rdr, n, err := stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.LessOrEqual(n, int64(100))
}

func (suite *DatabricksTests) TestConcurrentStatements() {
	stmt2, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt2)

	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT 1 as col1"))
	suite.Require().NoError(stmt2.SetSqlQuery("SELECT 2 as col2"))

	rdr1, n1, err1 := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err1)
	defer rdr1.Release()

	rdr2, n2, err2 := stmt2.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err2)
	defer rdr2.Release()

	suite.checkRowCount(1, n1)
	suite.checkRowCount(1, n2)

	suite.True(rdr1.Next())
	suite.True(rdr2.Next())

	rec1 := rdr1.RecordBatch()
	rec2 := rdr2.RecordBatch()

	suite.Equal("col1", rec1.ColumnName(0))
	suite.Equal("col2", rec2.ColumnName(0))
}

func (suite *DatabricksTests) TestLargeResultSet() {
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT id FROM range(1000)"))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.checkRowCount(1000, n)

	var totalRows int64
	for rdr.Next() {
		totalRows += rdr.RecordBatch().NumRows()
	}

	suite.Greater(totalRows, int64(0), "Should receive at least some rows")
	suite.NoError(rdr.Err())
}

func TestDriverCreation(t *testing.T) {
	drv := databricks.NewDriver(memory.DefaultAllocator)
	assert.NotNil(t, drv)
}

func TestDatabaseCreation(t *testing.T) {
	drv := databricks.NewDriver(memory.DefaultAllocator)

	opts := map[string]string{
		databricks.OptionServerHostname: "test-hostname",
		databricks.OptionHTTPPath:       "/sql/1.0/warehouses/test",
		databricks.OptionAccessToken:    "test-token",
	}

	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)
	assert.NotNil(t, db)

	defer validation.CheckedClose(t, db)
	assert.NoError(t, err)
}

func TestDatabaseCreationWithAllOptions(t *testing.T) {
	drv := databricks.NewDriver(memory.DefaultAllocator)

	opts := map[string]string{
		databricks.OptionServerHostname:      "test-hostname",
		databricks.OptionHTTPPath:            "/sql/1.0/warehouses/test",
		databricks.OptionAccessToken:         "test-token",
		databricks.OptionPort:                "443",
		databricks.OptionCatalog:             "test_catalog",
		databricks.OptionSchema:              "test_schema",
		databricks.OptionQueryTimeout:        "30s",
		databricks.OptionMaxRows:             "1000",
		databricks.OptionQueryRetryCount:     "3",
		databricks.OptionDownloadThreadCount: "4",
		databricks.OptionSSLMode:             "require",
	}

	db, err := drv.NewDatabase(opts)
	require.NoError(t, err)
	assert.NotNil(t, db)

	getSetDB, ok := db.(adbc.GetSetOptions)
	require.True(t, ok)

	value, err := getSetDB.GetOption(databricks.OptionCatalog)
	require.NoError(t, err)
	assert.Equal(t, "test_catalog", value)

	err = getSetDB.SetOption(databricks.OptionQueryTimeout, "60s")
	require.NoError(t, err)

	value, err = getSetDB.GetOption(databricks.OptionQueryTimeout)
	require.NoError(t, err)
	assert.Equal(t, "1m0s", value)

	defer validation.CheckedClose(t, db)
}

func (suite *DatabricksTests) TestConnectionManagement() {
	cnxn2, err := suite.db.Open(suite.ctx)
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), cnxn2)

	stmt1, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt1)

	stmt2, err := cnxn2.NewStatement()
	suite.Require().NoError(err)
	defer validation.CheckedClose(suite.T(), stmt2)

	suite.Require().NoError(stmt1.SetSqlQuery("SELECT 'connection1' as source"))
	suite.Require().NoError(stmt2.SetSqlQuery("SELECT 'connection2' as source"))

	rdr1, _, err := stmt1.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr1.Release()

	rdr2, _, err := stmt2.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr2.Release()

	suite.True(rdr1.Next())
	suite.True(rdr2.Next())
}

func (suite *DatabricksTests) TestDatabaseOptions() {
	testCases := []struct {
		name   string
		option string
		value  string
	}{
		{"Port", databricks.OptionPort, "443"},
		{"SSLMode", databricks.OptionSSLMode, "require"},
		{"QueryTimeout", databricks.OptionQueryTimeout, "1m0s"},
		{"MaxRows", databricks.OptionMaxRows, "5000"},
		{"QueryRetryCount", databricks.OptionQueryRetryCount, "5"},
		{"DownloadThreadCount", databricks.OptionDownloadThreadCount, "8"},
	}

	for _, tc := range testCases {
		suite.Run(tc.name, func() {
			dbOptions := suite.Quirks.DatabaseOptions()
			dbOptions[tc.option] = tc.value

			db, err := suite.driver.NewDatabase(dbOptions)
			suite.NoError(err)
			defer validation.CheckedClose(suite.T(), db)

			getSetDB, ok := db.(adbc.GetSetOptions)
			suite.True(ok)

			value, err := getSetDB.GetOption(tc.option)
			suite.NoError(err)
			suite.Equal(tc.value, value)
		})
	}
}

// TestErrorHandling validates proper error handling for invalid SQL and missing tables.
func (suite *DatabricksTests) TestErrorHandling() {
	suite.Run("InvalidSQL", func() {
		suite.Require().NoError(suite.stmt.SetSqlQuery("INVALID SQL SYNTAX"))
		_, _, err := suite.stmt.ExecuteQuery(suite.ctx)
		suite.Error(err, "Should return error for invalid SQL")
	})

	suite.Run("NonExistentTable", func() {
		suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT * FROM non_existent_table_12345"))
		_, _, err := suite.stmt.ExecuteQuery(suite.ctx)
		suite.Error(err, "Should return error for non-existent table")
	})

}

func (suite *DatabricksTests) TestTimestampPrecision() {
	query := "SELECT CAST('2023-01-01 12:00:00.123456' AS TIMESTAMP) as ts_col"

	suite.Require().NoError(suite.stmt.SetSqlQuery(query))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.checkRowCount(1, n)
	suite.True(rdr.Next())
	rec := rdr.RecordBatch()

	suite.Equal(int64(1), rec.NumCols())
	suite.Equal("ts_col", rec.ColumnName(0))

	field := rec.Schema().Field(0)
	suite.Equal(arrow.TIMESTAMP, field.Type.ID())

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *DatabricksTests) TestDecimalTypes() {
	query := "SELECT CAST(123.45 AS DECIMAL(10,2)) as decimal_col, CAST(999.999 AS DECIMAL(6,3)) as decimal_col2"

	suite.Require().NoError(suite.stmt.SetSqlQuery(query))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.checkRowCount(1, n)
	suite.True(rdr.Next())
	rec := rdr.RecordBatch()

	// Verify we got decimal columns
	suite.Equal(int64(2), rec.NumCols())
	suite.Equal("decimal_col", rec.ColumnName(0))
	suite.Equal("decimal_col2", rec.ColumnName(1))

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}
