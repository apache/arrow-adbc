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

package bigquery_test

import (
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/bigquery"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"os"
	"strings"
	"testing"
	"time"
)

type BigQueryQuirks struct {
	mem       *memory.CheckedAllocator
	authType  string
	authValue string
	// catalogName is the same as projectID
	catalogName string
	// schemaName is the same as datasetID
	schemaName string
}

func (q *BigQueryQuirks) SetupDriver(t *testing.T) adbc.Driver {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return driver.NewDriver(q.mem)
}

func (q *BigQueryQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	q.mem.AssertSize(t, 0)
}

func (q *BigQueryQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		driver.OptionStringAuthType:        q.authType,
		driver.OptionStringAuthCredentials: q.authValue,
		driver.OptionStringProjectID:       q.catalogName,
		driver.OptionStringDatasetID:       q.schemaName,
	}
}

func (q *BigQueryQuirks) getSqlTypeFromArrowType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "INTEGER"
	case arrow.FLOAT32:
		return "float64"
	case arrow.FLOAT64:
		return "double"
	case arrow.STRING:
		return "text"
	default:
		return ""
	}
}

func quoteTblName(name string) string {
	return "\"" + strings.ReplaceAll(name, "\"", "\"\"") + "\""
}

func (q *BigQueryQuirks) CreateSampleTable(tableName string, r arrow.Record) error {
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
		b.WriteString(q.getSqlTypeFromArrowType(f.Type))
	}

	b.WriteString(")")

	//db := sql.OpenDB(s.connector)
	//defer db.Close()
	//
	//if _, err := db.Exec(b.String()); err != nil {
	//	return err
	//}
	//
	//insertQuery := "INSERT INTO " + quoteTblName(tableName) + " VALUES ("
	//bindings := strings.Repeat("?,", int(r.NumCols()))
	//insertQuery += bindings[:len(bindings)-1] + ")"
	//
	//args := make([]interface{}, 0, r.NumCols())
	//for _, col := range r.Columns() {
	//	args = append(args, getArr(col))
	//}
	//
	//_, err := db.Exec(insertQuery, args...)
	//return err
	return nil
}

func (q *BigQueryQuirks) DropTable(cnxn adbc.Connection, tblname string) error {
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

func (q *BigQueryQuirks) Alloc() memory.Allocator                     { return q.mem }
func (q *BigQueryQuirks) BindParameter(_ int) string                  { return "?" }
func (q *BigQueryQuirks) SupportsBulkIngest(string) bool              { return true }
func (q *BigQueryQuirks) SupportsConcurrentStatements() bool          { return true }
func (q *BigQueryQuirks) SupportsCurrentCatalogSchema() bool          { return true }
func (q *BigQueryQuirks) SupportsExecuteSchema() bool                 { return true }
func (q *BigQueryQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *BigQueryQuirks) SupportsPartitionedData() bool               { return false }
func (q *BigQueryQuirks) SupportsStatistics() bool                    { return false }
func (q *BigQueryQuirks) SupportsTransactions() bool                  { return true }
func (q *BigQueryQuirks) SupportsGetParameterSchema() bool            { return false }
func (q *BigQueryQuirks) SupportsDynamicParameterBinding() bool       { return false }
func (q *BigQueryQuirks) SupportsErrorIngestIncompatibleSchema() bool { return false }
func (q *BigQueryQuirks) Catalog() string                             { return q.catalogName }
func (q *BigQueryQuirks) DBSchema() string                            { return q.schemaName }
func (q *BigQueryQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC BigQuery Driver - Go"
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
		return "BigQuery"
	}

	return nil
}

func (q *BigQueryQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
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

func createTempSchema(q *BigQueryQuirks, wait time.Duration) string {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	tmpDriver := driver.NewDriver(mem)
	db, err := tmpDriver.NewDatabase(q.DatabaseOptions())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	err = stmt.SetOption(driver.OptionBoolQueryUseLegacySQL, "false")
	if err != nil {
		panic(err)
	}

	schemaName := strings.ToUpper("ADBC_TESTING_" + strings.ReplaceAll(uuid.New().String(), "-", "_"))
	err = stmt.SetSqlQuery(fmt.Sprintf("CREATE SCHEMA %s", schemaName))
	if err != nil {
		panic(err)
	}
	_, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		panic(err)
	}

	// wait for some time before accessing it
	// BigQuery needs some time to make the table available
	// otherwise the query will fail with error saying the table cannot be found
	time.Sleep(wait)

	return schemaName
}

func createTempTable(q *BigQueryQuirks, schema string, wait time.Duration) string {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	tmpDriver := driver.NewDriver(mem)
	db, err := tmpDriver.NewDatabase(q.DatabaseOptions())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	err = stmt.SetOption(driver.OptionBoolQueryUseLegacySQL, "false")
	if err != nil {
		panic(err)
	}

	tableName := strings.ToUpper("ADBC_TABLE_" + strings.ReplaceAll(uuid.New().String(), "-", "_"))
	query := fmt.Sprintf("CREATE OR REPLACE TABLE `%s.%s` %s", q.schemaName, tableName, schema)
	err = stmt.SetSqlQuery(query)
	if err != nil {
		panic(err)
	}
	_, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		panic(err)
	}

	// wait for some time before accessing it
	// BigQuery needs some time to make the table available
	// otherwise the query will fail with error saying the table cannot be found
	time.Sleep(wait)

	return tableName
}

func dropTempSchema(q *BigQueryQuirks) {
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	tmpDriver := driver.NewDriver(mem)
	db, err := tmpDriver.NewDatabase(q.DatabaseOptions())
	if err != nil {
		panic(err)
	}
	defer db.Close()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer stmt.Close()

	query := fmt.Sprintf("DROP SCHEMA `%s.%s`", q.catalogName, q.schemaName)
	err = stmt.SetSqlQuery(query)
	if err != nil {
		panic(err)
	}
	_, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		panic(err)
	}
}

func withQuirks(t *testing.T, fn func(quirks *BigQueryQuirks)) {
	// set either env var for authentication
	//
	// - BIGQUERY_JSON_CREDENTIAL_FILE
	//     Path to the JSON credential file
	//
	// - BIGQUERY_JSON_CREDENTIAL_STRING
	//     Store the whole JSON credential content, something like
	//
	//     {
	//       "account": "",
	//       "client_id": "123456789012-1234567890abcdefabcdefabcdefabcd.apps.googleusercontent.com",
	//       "client_secret": "d-SECRETSECRETSECRETSECR",
	//       "refresh_token": "1//1234567890abcdefabcdefabcdef-abcdefabcd-abcdefabcdefabcdefabcdefab-abcdefabcdefabcdefabcdefabcdef-ab",
	//       "type": "authorized_user",
	//       "universe_domain": "googleapis.com"
	//     }
	authType := ""
	authValue := ""
	jsonCredentialString := os.Getenv("BIGQUERY_JSON_CREDENTIAL_STRING")
	if len(jsonCredentialString) > 0 {
		authType = driver.OptionValueAuthTypeJSONCredentialString
		authValue = jsonCredentialString
	} else {
		jsonCredentialFile := os.Getenv("BIGQUERY_JSON_CREDENTIAL_FILE")
		if len(jsonCredentialFile) > 0 {
			authType = driver.OptionValueAuthTypeJSONCredentialFile
			authValue = jsonCredentialFile
		} else {
			t.Skip("no BIGQUERY_JSON_CREDENTIAL_STRING or BIGQUERY_JSON_CREDENTIAL_FILE defined, skip bigquery driver tests")
		}
	}

	// env var BIGQUERY_PROJECT_ID should be set
	//
	// for example, the sample table provide by Google has the following values
	// https://cloud.google.com/bigquery/public-data#sample_tables
	//
	// export BIGQUERY_PROJECT_ID=bigquery-public-data
	projectID := os.Getenv("BIGQUERY_PROJECT_ID")
	if projectID == "" {
		t.Skip("no BIGQUERY_PROJECT_ID defined, skip bigquery driver tests")
	}

	// avoid multiple runs clashing by operating in a fresh schema and then
	// dropping that schema when we're done.
	q := &BigQueryQuirks{authType: authType, authValue: authValue, catalogName: projectID}
	q.schemaName = createTempSchema(q, 5*time.Second)
	t.Cleanup(func() {
		dropTempSchema(q)
	})
	fn(q)
}

// todo: finish other callbacks and make this validation test suite pass
//func TestValidation(t *testing.T) {
//	withQuirks(t, func(q *BigQueryQuirks) {
//		suite.Run(t, &validation.DatabaseTests{Quirks: q})
//		suite.Run(t, &validation.ConnectionTests{Quirks: q})
//		suite.Run(t, &validation.StatementTests{Quirks: q})
//	})
//}

func TestBigQuery(t *testing.T) {
	withQuirks(t, func(q *BigQueryQuirks) {
		suite.Run(t, &BigQueryTests{Quirks: q})
	})
}

// ---- Additional Tests --------------------

type BigQueryTests struct {
	suite.Suite

	Quirks *BigQueryQuirks

	ctx    context.Context
	driver adbc.Driver
	db     adbc.Database
	cnxn   adbc.Connection
	stmt   adbc.Statement
}

func (suite *BigQueryTests) SetupTest() {
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

func (suite *BigQueryTests) TearDownTest() {
	suite.NoError(suite.stmt.Close())
	suite.NoError(suite.cnxn.Close())
	suite.Quirks.TearDownDriver(suite.T(), suite.driver)
	suite.cnxn = nil
	suite.NoError(suite.db.Close())
	suite.db = nil
	suite.driver = nil
}

func (suite *BigQueryTests) TestEmptyResultSet() {
	// Google enforces `FROM` when `WHERE` appears in a query
	tableName := createTempTable(suite.Quirks, "(int64s INT, text STRING)", 1*time.Second)
	query := fmt.Sprintf("SELECT 42 FROM `%s.%s` WHERE 1=0", suite.Quirks.schemaName, tableName)
	suite.Require().NoError(suite.stmt.SetSqlQuery(query))
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
