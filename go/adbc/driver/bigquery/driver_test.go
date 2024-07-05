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
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/bigquery"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"github.com/apache/arrow/go/v17/arrow/decimal256"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
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

func getSqlTypeFromArrowField(f arrow.Field) string {
	switch f.Type.ID() {
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.UINT8, arrow.INT8, arrow.UINT16, arrow.INT16, arrow.UINT32, arrow.INT32, arrow.UINT64, arrow.INT64:
		return "INTEGER"
	case arrow.FLOAT32, arrow.FLOAT64:
		return "FLOAT64"
	case arrow.STRING:
		return "STRING"
	case arrow.BINARY, arrow.FIXED_SIZE_BINARY:
		return "BYTES"
	case arrow.DATE32, arrow.DATE64:
		return "DATE"
	case arrow.TIMESTAMP:
		return "TIMESTAMP"
	case arrow.TIME32, arrow.TIME64:
		return "TIME"
	case arrow.INTERVAL_MONTHS:
		return "INTERVAL_MONTHS"
	case arrow.DECIMAL128:
		return "NUMERIC"
	case arrow.DECIMAL256:
		return "BIGNUMERIC"
	case arrow.LIST:
		elem := getSqlTypeFromArrowField(f.Type.(*arrow.ListType).ElemField())
		return "ARRAY<" + elem + ">"
	case arrow.STRUCT:
		fields := f.Type.(*arrow.StructType).Fields()
		childTypes := make([]string, len(fields))
		for i, field := range fields {
			childTypes[i] = fmt.Sprintf("%s %s", field.Name, getSqlTypeFromArrowField(field))
		}
		return fmt.Sprintf("STRUCT<%s>", strings.Join(childTypes, ","))
	default:
		return ""
	}
}

func (q *BigQueryQuirks) quoteTblName(name string) string {
	return fmt.Sprintf("`%s.%s`", q.schemaName, strings.ReplaceAll(name, "\"", "\"\""))
}

func (q *BigQueryQuirks) CreateSampleTableWithRecords(tableName string, r arrow.Record) error {
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE TABLE ")
	b.WriteString(q.quoteTblName(tableName))
	b.WriteString(" (")

	for i := 0; i < int(r.NumCols()); i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		f := r.Schema().Field(i)
		b.WriteString(f.Name)
		b.WriteByte(' ')
		b.WriteString(getSqlTypeFromArrowField(f))
	}
	b.WriteString(")")

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

	creationQuery := b.String()
	err = stmt.SetSqlQuery(creationQuery)
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
	time.Sleep(5 * time.Second)

	insertQuery := "INSERT INTO " + q.quoteTblName(tableName) + " VALUES ("
	bindings := strings.Repeat("?,", int(r.NumCols()))
	insertQuery += bindings[:len(bindings)-1] + ")"
	err = stmt.Bind(ctx, r)
	if err != nil {
		return err
	}
	err = stmt.SetSqlQuery(insertQuery)
	if err != nil {
		return err
	}
	rdr, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}

	rdr.Release()
	return nil
}

func (q *BigQueryQuirks) CreateSampleTableWithStreams(tableName string, rdr array.RecordReader) error {
	var b strings.Builder
	b.WriteString("CREATE OR REPLACE TABLE ")
	b.WriteString(q.quoteTblName(tableName))
	b.WriteString(" (")

	for i := 0; i < rdr.Schema().NumFields(); i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		f := rdr.Schema().Field(i)
		b.WriteString(f.Name)
		b.WriteByte(' ')
		b.WriteString(getSqlTypeFromArrowField(f))
	}
	b.WriteString(")")

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

	creationQuery := b.String()
	err = stmt.SetSqlQuery(creationQuery)
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
	time.Sleep(5 * time.Second)

	insertQuery := "INSERT INTO " + q.quoteTblName(tableName) + " VALUES ("
	bindings := strings.Repeat("?,", rdr.Schema().NumFields())
	insertQuery += bindings[:len(bindings)-1] + ")"
	err = stmt.BindStream(ctx, rdr)
	if err != nil {
		return err
	}
	err = stmt.SetSqlQuery(insertQuery)
	if err != nil {
		return err
	}
	res, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}

	res.Release()
	return nil
}

func (q *BigQueryQuirks) DropTable(cnxn adbc.Connection, tblname string) error {
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return err
	}
	defer stmt.Close()

	if err = stmt.SetSqlQuery(`DROP TABLE IF EXISTS ` + q.quoteTblName(tblname)); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(context.Background())
	return err
}

func (q *BigQueryQuirks) Alloc() memory.Allocator                     { return q.mem }
func (q *BigQueryQuirks) BindParameter(_ int) string                  { return "?" }
func (q *BigQueryQuirks) SupportsBulkIngest(string) bool              { return false }
func (q *BigQueryQuirks) SupportsConcurrentStatements() bool          { return false }
func (q *BigQueryQuirks) SupportsCurrentCatalogSchema() bool          { return false }
func (q *BigQueryQuirks) SupportsExecuteSchema() bool                 { return false }
func (q *BigQueryQuirks) SupportsGetSetOptions() bool                 { return true }
func (q *BigQueryQuirks) SupportsPartitionedData() bool               { return false }
func (q *BigQueryQuirks) SupportsStatistics() bool                    { return false }
func (q *BigQueryQuirks) SupportsTransactions() bool                  { return false }
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

func (q *BigQueryQuirks) createTempSchema(wait time.Duration) string {
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

func (q *BigQueryQuirks) dropTempSchema() {
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

func samplePrimitiveTypeFields() []arrow.Field {
	return []arrow.Field{
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
	}
}

func samplePrimitiveTypeSchema() (*arrow.Schema, *arrow.Schema) {
	primitiveFields := samplePrimitiveTypeFields()
	input := arrow.NewSchema(primitiveFields, nil)
	// BigQuery only has time64[us], timestamp[us, tz=UTC] type
	primitiveFields[6].Type = arrow.FixedWidthTypes.Time64us
	primitiveFields[7].Type = arrow.FixedWidthTypes.Time64us
	primitiveFields[8].Type = arrow.FixedWidthTypes.Time64us
	primitiveFields[9].Type = arrow.FixedWidthTypes.Time64us
	primitiveFields[10].Type = arrow.FixedWidthTypes.Timestamp_us
	primitiveFields[11].Type = arrow.FixedWidthTypes.Timestamp_us
	primitiveFields[12].Type = arrow.FixedWidthTypes.Timestamp_us
	expected := arrow.NewSchema(primitiveFields, nil)
	return input, expected
}

func buildSamplePrimitiveTypeRecord(mem memory.Allocator, schema, bigquery *arrow.Schema) (arrow.Record, arrow.Record) {
	bldr := array.NewRecordBuilder(mem, schema)
	defer bldr.Release()

	bldr2 := array.NewRecordBuilder(mem, bigquery)
	defer bldr2.Release()

	int64s := []int64{-1, 0, 25}
	float64s := []float64{-1.1, 0, 25.95}
	stringData := []string{"first", "second", "third"}
	bytesData := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	booleans := []bool{true, false, true}
	date32s := []arrow.Date32{1, 2, 3}
	arrowTime64s := []arrow.Time64{1, 2, 3}
	arrowTime32s := []arrow.Time32{1, 2, 3}
	arrowTimestampNs := []arrow.Timestamp{1000000000, 2000000000, 3000000000}
	arrowTimestampUs := []arrow.Timestamp{1000000, 2000000, 3000000}
	arrowTimestampS := []arrow.Timestamp{1, 2, 3}

	bldr.Field(0).(*array.Int64Builder).AppendValues(int64s, nil)
	bldr.Field(1).(*array.Float64Builder).AppendValues(float64s, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues(stringData, nil)
	bldr.Field(3).(*array.BinaryBuilder).AppendValues(bytesData, nil)
	bldr.Field(4).(*array.BooleanBuilder).AppendValues(booleans, nil)
	bldr.Field(5).(*array.Date32Builder).AppendValues(date32s, nil)
	bldr.Field(6).(*array.Time64Builder).AppendValues(arrowTime64s, nil)
	bldr.Field(7).(*array.Time64Builder).AppendValues(arrowTime64s, nil)
	bldr.Field(8).(*array.Time32Builder).AppendValues(arrowTime32s, nil)
	bldr.Field(9).(*array.Time32Builder).AppendValues(arrowTime32s, nil)
	bldr.Field(10).(*array.TimestampBuilder).AppendValues(arrowTimestampNs, nil)
	bldr.Field(11).(*array.TimestampBuilder).AppendValues(arrowTimestampUs, nil)
	bldr.Field(12).(*array.TimestampBuilder).AppendValues(arrowTimestampS, nil)

	bigQueryTime32msAsTime64us := []arrow.Time64{1000, 2000, 3000}
	bigQueryTime32sAsTime64us := []arrow.Time64{1000000, 2000000, 3000000}
	bigQueryTimestamps := []arrow.Timestamp{1000000, 2000000, 3000000}
	bldr2.Field(0).(*array.Int64Builder).AppendValues(int64s, nil)
	bldr2.Field(1).(*array.Float64Builder).AppendValues(float64s, nil)
	bldr2.Field(2).(*array.StringBuilder).AppendValues(stringData, nil)
	bldr2.Field(3).(*array.BinaryBuilder).AppendValues(bytesData, nil)
	bldr2.Field(4).(*array.BooleanBuilder).AppendValues(booleans, nil)
	bldr2.Field(5).(*array.Date32Builder).AppendValues(date32s, nil)
	bldr2.Field(6).(*array.Time64Builder).AppendValues(arrowTime64s, nil)
	bldr2.Field(7).(*array.Time64Builder).AppendValues(arrowTime64s, nil)
	bldr2.Field(8).(*array.Time64Builder).AppendValues(bigQueryTime32msAsTime64us, nil)
	bldr2.Field(9).(*array.Time64Builder).AppendValues(bigQueryTime32sAsTime64us, nil)
	bldr2.Field(10).(*array.TimestampBuilder).AppendValues(bigQueryTimestamps, nil)
	bldr2.Field(11).(*array.TimestampBuilder).AppendValues(bigQueryTimestamps, nil)
	bldr2.Field(12).(*array.TimestampBuilder).AppendValues(bigQueryTimestamps, nil)

	return bldr.NewRecord(), bldr2.NewRecord()
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
	q.schemaName = q.createTempSchema(5 * time.Second)
	t.Cleanup(func() {
		q.dropTempSchema()
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

func (suite *BigQueryTests) TestNewDatabaseGetSetOptions() {
	key1, val1 := driver.OptionStringProjectID, "val1"
	key2, val2 := driver.OptionStringDatasetID, "val2"

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

func (suite *BigQueryTests) TestEmptyResultSet() {
	// Google enforces `FROM` when `WHERE` appears in a query
	tableName := createTempTable(suite.Quirks, "(int64s INTEGER)", 1*time.Second)
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

func (suite *BigQueryTests) TestSqlBulkInsertRecords() {
	bulkInsertTableName := "bulk_insertions"
	input, expected := samplePrimitiveTypeSchema()
	rec, expectedRec := buildSamplePrimitiveTypeRecord(suite.Quirks.Alloc(), input, expected)
	defer rec.Release()
	defer expectedRec.Release()

	err := suite.Quirks.CreateSampleTableWithRecords(bulkInsertTableName, rec)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY `col_int64` ASC", suite.Quirks.schemaName, bulkInsertTableName)))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	resultBind := rdr.Record()

	suite.Truef(array.RecordEqual(expectedRec, resultBind), "expected: %s\ngot: %s", expectedRec, resultBind)
	suite.False(rdr.Next())

	suite.Require().NoError(rdr.Err())
}

func (suite *BigQueryTests) TestSqlBulkInsertStreams() {
	bulkInsertTableName := "bulk_insertions_stream"
	input, expected := samplePrimitiveTypeSchema()
	rec, expectedRec := buildSamplePrimitiveTypeRecord(suite.Quirks.Alloc(), input, expected)
	defer rec.Release()
	defer expectedRec.Release()

	stream, err := array.NewRecordReader(input, []arrow.Record{rec})
	suite.Require().NoError(err)
	defer stream.Release()

	err = suite.Quirks.CreateSampleTableWithStreams(bulkInsertTableName, stream)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY `col_int64` ASC", suite.Quirks.schemaName, bulkInsertTableName)))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	resultBindStream := rdr.Record()

	suite.Truef(array.RecordEqual(expectedRec, resultBindStream), "expected: %s\ngot: %s", expectedRec, resultBindStream)
	suite.False(rdr.Next())

	suite.Require().NoError(rdr.Err())
}

func (suite *BigQueryTests) TestSqlIngestTimestampTypes() {
	tableName := "bulk_ingest_timestamps"
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, tableName))

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
	bldr.Field(1).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1000, 2000, 3000}, nil)
	bldr.Field(2).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(4).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(5).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)
	bldr.Field(6).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{1, 2, 3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	err := suite.Quirks.CreateSampleTableWithRecords(tableName, rec)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY `col_int64` ASC", suite.Quirks.schemaName, tableName)))
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
			Name: "col_timestamp_ns", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
			Nullable: true,
		},
		{
			Name: "col_timestamp_us", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
			Nullable: true,
		},
		{
			Name: "col_timestamp_ms", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
			Nullable: true,
		},
		{
			Name: "col_timestamp_s", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
			Nullable: true,
		},
		{
			Name: "col_timestamp_s_tz", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
			Nullable: true,
		},
		{
			Name: "col_timestamp_s_ntz", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"},
			Nullable: true,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_timestamp_ns": 1,
			"col_timestamp_us": 1,
			"col_timestamp_ms": 1000,
			"col_timestamp_s": 1000000,
			"col_timestamp_s_tz": 1000000,
			"col_timestamp_s_ntz": 1000000
		},
		{
			"col_int64": 2,
			"col_timestamp_ns": 2,
			"col_timestamp_us": 2,
			"col_timestamp_ms": 2000,
			"col_timestamp_s": 2000000,
			"col_timestamp_s_tz": 2000000,
			"col_timestamp_s_ntz": 2000000
		},
		{
			"col_int64": 3,
			"col_timestamp_ns": 3,
			"col_timestamp_us": 3,
			"col_timestamp_ms": 3000,
			"col_timestamp_s": 3000000,
			"col_timestamp_s_tz": 3000000,
			"col_timestamp_s_ntz": 3000000
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *BigQueryTests) TestSqlIngestDate64Type() {
	tableName := "bulk_ingest_date64"
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, tableName))

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

	err := suite.Quirks.CreateSampleTableWithRecords(tableName, rec)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY `col_int64` ASC", suite.Quirks.schemaName, tableName)))
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

func (suite *BigQueryTests) TestSqlIngestDecimal() {
	tableName := "bulk_ingest_decimal"
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, tableName))

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
		{
			Name: "col_decimal256_whole", Type: &arrow.Decimal256Type{Precision: 76, Scale: 0},
			Nullable: true,
		},
		{
			Name: "col_decimal256_fractional", Type: &arrow.Decimal256Type{Precision: 76, Scale: 4},
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

	bldr.Field(4).(*array.Decimal256Builder).AppendValues([]decimal256.Num{decimal256.FromI64(123), decimal256.FromI64(456), decimal256.FromI64(789)}, nil)
	d256num1, err := decimal256.FromString("123", 76, 4)
	suite.Require().NoError(err)
	d256num2, err := decimal256.FromString("456.7", 76, 4)
	suite.Require().NoError(err)
	d256num3, err := decimal256.FromString("891.01", 76, 4)
	suite.Require().NoError(err)
	bldr.Field(5).(*array.Decimal256Builder).AppendValues([]decimal256.Num{d256num1, d256num2, d256num3}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	err = suite.Quirks.CreateSampleTableWithRecords(tableName, rec)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY `col_int64` ASC", suite.Quirks.schemaName, tableName)))
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
			Name: "col_float64", Type: arrow.PrimitiveTypes.Float64,
			Nullable: true,
		},
		{
			Name: "col_decimal128_whole", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9},
			Nullable: true,
		},
		{
			Name: "col_decimal128_fractional", Type: &arrow.Decimal128Type{Precision: 38, Scale: 9},
			Nullable: true,
		},
		{
			Name: "col_decimal256_whole", Type: &arrow.Decimal256Type{Precision: 76, Scale: 38},
			Nullable: true,
		},
		{
			Name: "col_decimal256_fractional", Type: &arrow.Decimal256Type{Precision: 76, Scale: 38},
			Nullable: true,
		},
	}, nil)

	bldr2 := array.NewRecordBuilder(suite.Quirks.Alloc(), expectedSchema)
	defer bldr2.Release()

	bldr2.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	bldr2.Field(1).(*array.Float64Builder).AppendValues([]float64{1.2, 2.34, 3.456}, nil)

	expectedWholeD128Num1, err := decimal128.FromString("123", 38, 9)
	suite.Require().NoError(err)
	expectedWholeD128Num2, err := decimal128.FromString("456", 38, 9)
	suite.Require().NoError(err)
	expectedWholeD128Num3, err := decimal128.FromString("789", 38, 9)
	suite.Require().NoError(err)
	bldr2.Field(2).(*array.Decimal128Builder).AppendValues([]decimal128.Num{expectedWholeD128Num1, expectedWholeD128Num2, expectedWholeD128Num3}, nil)

	expectedD128Num1, err := decimal128.FromString("123", 38, 9)
	suite.Require().NoError(err)
	expectedD128Num2, err := decimal128.FromString("456.7", 38, 9)
	suite.Require().NoError(err)
	expectedD128Num3, err := decimal128.FromString("891.01", 38, 9)
	suite.Require().NoError(err)
	bldr2.Field(3).(*array.Decimal128Builder).AppendValues([]decimal128.Num{expectedD128Num1, expectedD128Num2, expectedD128Num3}, nil)

	expectedWholeD256Num1, err := decimal256.FromString("123", 76, 38)
	suite.Require().NoError(err)
	expectedWholeD256Num2, err := decimal256.FromString("456", 76, 38)
	suite.Require().NoError(err)
	expectedWholeD256Num3, err := decimal256.FromString("789", 76, 38)
	suite.Require().NoError(err)
	bldr2.Field(4).(*array.Decimal256Builder).AppendValues([]decimal256.Num{expectedWholeD256Num1, expectedWholeD256Num2, expectedWholeD256Num3}, nil)

	expectedD256Num1, err := decimal256.FromString("123", 76, 38)
	suite.Require().NoError(err)
	expectedD256Num2, err := decimal256.FromString("456.7", 76, 38)
	suite.Require().NoError(err)
	expectedD256Num3, err := decimal256.FromString("891.01", 76, 38)
	suite.Require().NoError(err)
	bldr2.Field(5).(*array.Decimal256Builder).AppendValues([]decimal256.Num{expectedD256Num1, expectedD256Num2, expectedD256Num3}, nil)

	expectedRec := bldr2.NewRecord()
	defer expectedRec.Release()

	suite.Truef(array.RecordEqual(expectedRec, result), "expected: %s\ngot: %s", expectedRec, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *BigQueryTests) TestSqlIngestListType() {
	tableName := "bulk_ingest_list"
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, tableName))

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_list_float64", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64),
			Nullable: true,
		},
		{
			Name: "col_list_str", Type: arrow.ListOf(arrow.BinaryTypes.String),
			Nullable: true,
		},
	}, nil)

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), sc)
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	f64listbldr := bldr.Field(1).(*array.ListBuilder)
	f64listvalbldr := f64listbldr.ValueBuilder().(*array.Float64Builder)
	f64Row1 := []float64{100.1, 100.2, 100.3}
	f64Row2 := []float64{200.1, 200.2}
	f64Row3 := []float64{300.1, 300.2, 300.3, 300.4}
	f64listvalbldr.AppendValues(f64Row1, nil)
	f64listvalbldr.AppendValues(f64Row2, nil)
	f64listvalbldr.AppendValues(f64Row3, nil)
	offsets := []int32{0, 3, 5, 9}
	valid := []bool{true, true, true}
	f64listbldr.AppendValues(offsets, valid)

	listbldr := bldr.Field(2).(*array.ListBuilder)
	listvalbldr := listbldr.ValueBuilder().(*array.StringBuilder)
	strRow1 := []string{"first_row_elem_1", "first_row_elem_2", "first_row_elem_3"}
	strRow2 := []string{"second_row_elem_1", "second_row_elem_2"}
	strRow3 := []string{"third_row_elem_1", "third_row_elem_2", "third_row_elem_3", "third_row_elem_4"}
	listvalbldr.AppendValues(strRow1, nil)
	listvalbldr.AppendValues(strRow2, nil)
	listvalbldr.AppendValues(strRow3, nil)
	listbldr.AppendValues(offsets, valid)

	rec := bldr.NewRecord()
	defer rec.Release()

	err := suite.Quirks.CreateSampleTableWithRecords(tableName, rec)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY `col_int64` ASC", suite.Quirks.schemaName, tableName)))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	// Array cannot be NULL
	expectedSchema := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_list_float64", Type: arrow.ListOf(arrow.PrimitiveTypes.Float64),
			Nullable: false,
		},
		{
			Name: "col_list_str", Type: arrow.ListOf(arrow.BinaryTypes.String),
			Nullable: false,
		},
	}, nil)

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), expectedSchema, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_list_float64": [100.1, 100.2, 100.3],
			"col_list_str": ["first_row_elem_1", "first_row_elem_2", "first_row_elem_3"]
		},
		{
			"col_int64": 2,
			"col_list_float64": [200.1, 200.2],
			"col_list_str": ["second_row_elem_1", "second_row_elem_2"]
		},
		{
			"col_int64": 3,
			"col_list_float64": [300.1, 300.2, 300.3, 300.4],
			"col_list_str": ["third_row_elem_1", "third_row_elem_2", "third_row_elem_3", "third_row_elem_4"]
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}

func (suite *BigQueryTests) TestSqlIngestStructType() {
	tableName := "bulk_ingest_struct"
	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, tableName))

	sc := arrow.NewSchema([]arrow.Field{
		{
			Name: "col_int64", Type: arrow.PrimitiveTypes.Int64,
			Nullable: true,
		},
		{
			Name: "col_struct", Type: arrow.StructOf([]arrow.Field{
				{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "age", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			}...),
			Nullable: true,
		},
		{
			Name: "col_struct_of_struct", Type: arrow.StructOf([]arrow.Field{
				{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "nested", Type: arrow.StructOf([]arrow.Field{
					{Name: "nested_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "ready", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				}...),
					Nullable: true,
				},
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
	struct2bldr.AppendValues([]bool{true, true, true})
	struct2bldr.FieldBuilder(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)

	struct3bldr := struct2bldr.FieldBuilder(1).(*array.StructBuilder)
	struct3bldr.AppendValues([]bool{true, true, true})
	struct3bldr.FieldBuilder(0).(*array.Int64Builder).AppendValues([]int64{1, 2, 3}, nil)
	struct3bldr.FieldBuilder(1).(*array.BooleanBuilder).AppendValues([]bool{true, false, false}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	err := suite.Quirks.CreateSampleTableWithRecords(tableName, rec)
	suite.Require().NoError(err)

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("SELECT * FROM `%s.%s` ORDER BY `col_int64` ASC", suite.Quirks.schemaName, tableName)))
	rdr, n, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.EqualValues(3, n)
	suite.True(rdr.Next())
	result := rdr.Record()

	expectedRecord, _, err := array.RecordFromJSON(suite.Quirks.Alloc(), sc, bytes.NewReader([]byte(`
	[
		{
			"col_int64": 1,
			"col_struct": {
				"age": 10,
				"name": "one"
			},
			"col_struct_of_struct": {
				"id": 1,
				"nested": {
					"nested_id": 1,
					"ready": true
				}
			}
		},
		{
			"col_int64": 2,
			"col_struct": {
				"age": 20,
				"name": "two"
			},
			"col_struct_of_struct": {
				"id": 2,
				"nested": {
					"nested_id": 2,
					"ready": false
				}
			}
		},
		{
			"col_int64": 3,
			"col_struct": {
				"age": 30,
	           "name": "three"
			},
			"col_struct_of_struct": {
				"id": 3,
				"nested": {
					"nested_id": 3,
					"ready": false
				}
			}
		}
	]
	`)))
	suite.Require().NoError(err)
	defer expectedRecord.Release()

	suite.Truef(array.RecordEqual(expectedRecord, result), "expected: %s\ngot: %s", expectedRecord, result)

	suite.False(rdr.Next())
	suite.Require().NoError(rdr.Err())
}
