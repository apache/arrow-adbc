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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/bigquery"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
)

type BigQueryQuirks struct {
	ctx    context.Context
	mem    *memory.CheckedAllocator
	client *bigquery.Client
	// catalogName is the same as projectID
	catalogName string
	// schemaName is the same as datasetID
	schemaName string
}

func (q *BigQueryQuirks) CreateSampleTable(tableName string, r arrow.Record) (err error) {
	var buf bytes.Buffer

	w, err := pqarrow.NewFileWriter(
		r.Schema(),
		&buf,
		parquet.NewWriterProperties(parquet.WithAllocator(q.mem)),
		pqarrow.NewArrowWriterProperties(pqarrow.WithAllocator(q.mem)),
	)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, w.Close())
	}()

	if err = w.Write(r); err != nil {
		return err
	}

	if err = w.Close(); err != nil {
		return err
	}

	src := bigquery.NewReaderSource(&buf)
	src.SourceFormat = bigquery.Parquet

	loader := q.client.Dataset(q.schemaName).Table(tableName).LoaderFrom(src)
	job, err := loader.Run(q.ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(q.ctx)
	return errors.Join(err, status.Err())
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
		driver.OptionStringProjectID: q.catalogName,
		driver.OptionStringDatasetID: q.schemaName,
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
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := cnxn.Close()
		if err != nil {
			panic(err)
		}
	}()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			panic(err)
		}
	}()

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
	defer func() {
		err := db.Close()
		if err != nil {
			panic(err)
		}
	}()

	cnxn, err := db.Open(ctx)
	if err != nil {
		panic(err)
	}
	defer func() {
		err := cnxn.Close()
		if err != nil {
			panic(err)
		}
	}()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		panic(err)
	}
	defer func() {
		err := stmt.Close()
		if err != nil {
			panic(err)
		}
	}()

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
	defer func() {
		if err = stmt.Close(); err != nil {
			panic(err)
		}
	}()

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
func (q *BigQueryQuirks) SupportsCurrentCatalogSchema() bool          { return true }
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
	metadata := map[string]string{
		"DefaultValueExpression": "",
		"Description":            "",
		"Repeated":               "false",
		"Required":               "false",
	}

	switch dt.ID() {
	case arrow.STRING:
		metadata["Collation"] = ""
		metadata["MaxLength"] = "0"
		metadata["Type"] = "STRING"
	case arrow.INT64:
		metadata["Type"] = "INTEGER"
	}

	return arrow.MetadataFrom(metadata)
}

func createTempSchema(ctx context.Context, client *bigquery.Client) string {
	schemaName := strings.ToUpper("ADBC_TESTING_" + strings.ReplaceAll(uuid.New().String(), "-", "_"))

	dataset := client.Dataset(schemaName)
	err := dataset.Create(ctx, nil)
	if err != nil {
		panic(err)
	}

	return schemaName
}

func dropTempSchema(ctx context.Context, client *bigquery.Client, schemaName string) {
	if err := client.Dataset(schemaName).DeleteWithContents(ctx); err != nil {
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
	ctx := context.Background()

	// Detect ProjectID from Application Default Credentials
	// See: https://cloud.google.com/docs/authentication/application-default-credentials
	// Can be overridden by setting env var GOOGLE_CLOUD_PROJECT
	client, err := bigquery.NewClient(ctx, bigquery.DetectProjectID)
	if err != nil {
		t.Skipf("failed to detect client config from environment, skip bigquery driver tests: %s", err)
	}

	// avoid multiple runs clashing by operating in a fresh schema and then
	// dropping that schema when we're done.
	q := &BigQueryQuirks{
		ctx:         ctx,
		client:      client,
		catalogName: client.Project(),
		schemaName:  createTempSchema(ctx, client),
	}

	t.Cleanup(func() {
		dropTempSchema(ctx, client, q.schemaName)
	})

	fn(q)
}

// todo: finish other callbacks and make this validation test suite pass
func TestValidation(t *testing.T) {
	withQuirks(t, func(q *BigQueryQuirks) {
		suite.Run(t, &validation.DatabaseTests{Quirks: q})
		suite.Run(t, &validation.ConnectionTests{Quirks: q})
		suite.Run(t, &validation.StatementTests{Quirks: q})
	})
}

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

func (suite *BigQueryTests) TestDropSchema() {
	// Unique schema name
	schema := fmt.Sprintf("%s_x", suite.Quirks.DBSchema())

	// Create unique schema to drop via a query
	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("CREATE SCHEMA %s", schema)))
	rdr, _, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	rdr.Release()

	suite.Require().NoError(suite.stmt.SetSqlQuery(fmt.Sprintf("DROP SCHEMA %s CASCADE", schema)))
	rdr, _, err = suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	rdr.Release()

	// We expect an error as the schema should not exist
	suite.Require().Error(suite.Quirks.client.Dataset(schema).DeleteWithContents(suite.Quirks.ctx))
}

func (suite *BigQueryTests) TestCreateView() {
	// Create unique schema to drop via a query
	suite.Require().NoError(suite.stmt.SetSqlQuery("CREATE TABLE IF NOT EXISTS a (id int)"))
	rdr, _, err := suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	rdr.Release()

	suite.Require().NoError(suite.stmt.SetSqlQuery("CREATE VIEW IF NOT EXISTS a_view AS SELECT * FROM a"))
	rdr, _, err = suite.stmt.ExecuteQuery(suite.ctx)
	suite.Require().NoError(err)
	rdr.Release()
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
	defer validation.CheckedClose(suite.T(), db)

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
	suite.Require().NoError(suite.stmt.SetSqlQuery("SELECT * FROM UNNEST([])"))
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

func (suite *BigQueryTests) TestMetadataGetObjectsColumnsXdbc() {

	suite.Require().NoError(suite.Quirks.DropTable(suite.cnxn, "bulk_ingest"))

	bldr := array.NewRecordBuilder(suite.Quirks.Alloc(), arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
			{Name: "decs", Type: &arrow.Decimal128Type{Precision: 10, Scale: 2}, Nullable: false},
			{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "timestamps", Type: arrow.FixedWidthTypes.Timestamp_us, Nullable: true},
		}, nil))
	defer bldr.Release()

	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{42, -42, 0}, nil)
	bldr.Field(1).(*array.Decimal128Builder).AppendValues([]decimal128.Num{decimal128.FromI64(42), decimal128.FromI64(-42), decimal128.FromI64(0)}, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"foo", "", ""}, []bool{true, false, true})
	bldr.Field(3).(*array.TimestampBuilder).AppendValues([]arrow.Timestamp{arrow.Timestamp(1), arrow.Timestamp(2), arrow.Timestamp(3)}, nil)

	rec := bldr.NewRecord()
	defer rec.Release()

	suite.Require().NoError(suite.Quirks.CreateSampleTable("bulk_ingest", rec))
	suite.Require().NoError(suite.Quirks.CreateSampleTable("bulk_ingest2", rec))

	_, err := suite.Quirks.client.Query(
		fmt.Sprintf(
			"ALTER TABLE %s.bulk_ingest ADD PRIMARY KEY (int64s, decs) NOT ENFORCED",
			suite.Quirks.schemaName),
	).Read(suite.ctx)
	suite.Require().NoError(err)

	_, err = suite.Quirks.client.Query(
		fmt.Sprintf(
			"ALTER TABLE %s.bulk_ingest2 ADD PRIMARY KEY (int64s, decs) NOT ENFORCED",
			suite.Quirks.schemaName),
	).Read(suite.ctx)
	suite.Require().NoError(err)

	_, err = suite.Quirks.client.Query(
		fmt.Sprintf(`
		ALTER TABLE %[1]s.bulk_ingest
		ADD CONSTRAINT test_fk_name FOREIGN KEY (int64s, decs)
		REFERENCES %[1]s.bulk_ingest2(int64s, decs) NOT ENFORCED`,
			suite.Quirks.schemaName),
	).Read(suite.ctx)
	suite.Require().NoError(err)

	var (
		expectedColnames              = []string{"int64s", "decs", "strings", "timestamps"}
		expectedPositions             = []string{"1", "2", "3", "4"}
		expectedComments              = []string{"", "", "", ""}
		expectedXdbcDataType          = []string{"9", "23", "13", "18"}
		expectedXdbcTypeName          = []string{"INTEGER", "NUMERIC", "STRING", "TIMESTAMP"}
		expectedXdbcColumnSize        = []string{"0", "0", "0", "0"} // TODO: Should be supported, not included in API resp
		expectedXdbcDecimalDigits     = []string{"0", "0", "0", "0"} // TODO: Should be supported, not included in API resp
		expectedXdbcNumPrecRadix      = []string{"0", "0", "0", "0"} // Not supported
		expectedXdbcNullable          = []string{"0", "0", "1", "1"}
		expectedXdbcColumnDef         = []string{"", "", "", ""} // Not supported
		expectedXdbcSqlDataType       = []string{"-5", "3", "12", "93"}
		expectedXdbcDateTimeSub       = []string{"0", "0", "0", "0"} // Only for RANGE
		expectedXdbcCharOctetLen      = []string{"0", "0", "0", "0"} // Only for BYTES
		expectedXdbcIsNullable        = []string{"NO", "NO", "YES", "YES"}
		expectedXdbcScopeCatalog      = []string{suite.Quirks.catalogName, suite.Quirks.catalogName, suite.Quirks.catalogName, suite.Quirks.catalogName}
		expectedXdbcScopeSchema       = []string{suite.Quirks.schemaName, suite.Quirks.schemaName, suite.Quirks.schemaName, suite.Quirks.schemaName}
		expectedXdbcScopeTable        = []string{"bulk_ingest", "bulk_ingest", "bulk_ingest", "bulk_ingest"}
		expectedXdbcIsAutoIncrement   = []bool{false, false, false, false} // Not supported
		expectedXdbcIsGeneratedColumn = []bool{false, false, false, false} // Not supported
		expectedConstraints           = []struct {
			Name, Type string
		}{
			{Type: "PRIMARY KEY"},
			{Name: "test_fk_name", Type: "FOREIGN KEY"},
		}
	)

	rdr, err := suite.cnxn.GetObjects(suite.ctx, adbc.ObjectDepthColumns, nil, nil, nil, nil, nil)
	suite.Require().NoError(err)
	defer rdr.Release()

	suite.Truef(adbc.GetObjectsSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", adbc.GetObjectsSchema, rdr.Schema())
	suite.True(rdr.Next())
	rec = rdr.Record()
	suite.Greater(rec.NumRows(), int64(0))
	suite.True(rec.Schema().Equal(adbc.GetObjectsSchema))
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

		colnames              = make([]string, 0)
		positions             = make([]string, 0)
		comments              = make([]string, 0)
		constraints           = make([]struct{ Name, Type string }, 0)
		xdbcDataTypes         = make([]string, 0)
		xdbcTypeNames         = make([]string, 0)
		xdbcColumnSize        = make([]string, 0)
		xdbcDecimalDigits     = make([]string, 0)
		xdbcNumPrecRadixs     = make([]string, 0)
		xdbcNullables         = make([]string, 0)
		xdbcColumnDef         = make([]string, 0)
		xdbcSqlDataTypes      = make([]string, 0)
		xdbcDateTimeSub       = make([]string, 0)
		xdbcCharOctetLen      = make([]string, 0)
		xdbcIsNullables       = make([]string, 0)
		xdbcScopeCatalog      = make([]string, 0)
		xdbcScopeSchema       = make([]string, 0)
		xdbcScopeTable        = make([]string, 0)
		xdbcIsAutoIncrement   = make([]bool, 0)
		xdbcIsGeneratedColumn = make([]bool, 0)
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
						xdbcTypeNames = append(xdbcTypeNames, dataType)

						// these are column size attributes used for either precision for numbers OR the length for text
						maxLenOrPrecision := tableColumns.Field(5).(*array.Int32).Value(int(colIdx))
						xdbcColumnSize = append(xdbcColumnSize, strconv.Itoa(int(maxLenOrPrecision)))

						scale := tableColumns.Field(6).(*array.Int16).Value(int(colIdx))
						xdbcDecimalDigits = append(xdbcDecimalDigits, strconv.Itoa(int(scale)))

						radix := tableColumns.Field(7).(*array.Int16).Value(int(colIdx))
						xdbcNumPrecRadixs = append(xdbcNumPrecRadixs, strconv.Itoa(int(radix)))

						isnull := tableColumns.Field(8).(*array.Int16).Value(int(colIdx))
						xdbcNullables = append(xdbcNullables, strconv.Itoa(int(isnull)))

						xdbcColumnDef = append(xdbcColumnDef, tableColumns.Field(9).(*array.String).Value(int(colIdx)))

						sqlType := tableColumns.Field(10).(*array.Int16).Value(int(colIdx))
						xdbcSqlDataTypes = append(xdbcSqlDataTypes, strconv.Itoa(int(sqlType)))

						dtPrec := tableColumns.Field(11).(*array.Int16).Value(int(colIdx))
						xdbcDateTimeSub = append(xdbcDateTimeSub, strconv.Itoa(int(dtPrec)))

						charOctetLen := tableColumns.Field(12).(*array.Int32).Value(int(colIdx))
						xdbcCharOctetLen = append(xdbcCharOctetLen, strconv.Itoa(int(charOctetLen)))

						xdbcIsNullables = append(xdbcIsNullables, tableColumns.Field(13).(*array.String).Value(int(colIdx)))

						xdbcScopeCatalog = append(xdbcScopeCatalog, tableColumns.Field(14).(*array.String).Value(int(colIdx)))
						xdbcScopeSchema = append(xdbcScopeSchema, tableColumns.Field(15).(*array.String).Value(int(colIdx)))
						xdbcScopeTable = append(xdbcScopeTable, tableColumns.Field(16).(*array.String).Value(int(colIdx)))

						xdbcIsAutoIncrement = append(xdbcIsAutoIncrement, tableColumns.Field(17).(*array.Boolean).Value(int(colIdx)))
						xdbcIsGeneratedColumn = append(xdbcIsGeneratedColumn, tableColumns.Field(18).(*array.Boolean).Value(int(colIdx)))
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
	suite.ElementsMatch(expectedXdbcDataType, xdbcDataTypes)
	suite.ElementsMatch(expectedXdbcTypeName, xdbcTypeNames)
	suite.ElementsMatch(expectedXdbcColumnSize, xdbcColumnSize)
	suite.ElementsMatch(expectedXdbcDecimalDigits, xdbcDecimalDigits)
	suite.ElementsMatch(expectedXdbcNumPrecRadix, xdbcNumPrecRadixs)
	suite.ElementsMatch(expectedXdbcNullable, xdbcNullables)
	suite.ElementsMatch(expectedXdbcColumnDef, xdbcColumnDef)
	suite.ElementsMatch(expectedXdbcSqlDataType, xdbcSqlDataTypes)
	suite.ElementsMatch(expectedXdbcDateTimeSub, xdbcDateTimeSub)
	suite.ElementsMatch(expectedXdbcCharOctetLen, xdbcCharOctetLen)
	suite.ElementsMatch(expectedXdbcIsNullable, xdbcIsNullables)
	suite.ElementsMatch(expectedXdbcScopeCatalog, xdbcScopeCatalog)
	suite.ElementsMatch(expectedXdbcScopeSchema, xdbcScopeSchema)
	suite.ElementsMatch(expectedXdbcScopeTable, xdbcScopeTable)
	suite.ElementsMatch(expectedXdbcIsAutoIncrement, xdbcIsAutoIncrement)
	suite.ElementsMatch(expectedXdbcIsGeneratedColumn, xdbcIsGeneratedColumn)
	suite.ElementsMatch(expectedConstraints, constraints)

}

func (suite *BigQueryTests) TestStatementExecuteQueryIngest() {
	ctx := context.Background()
	conn := suite.cnxn
	defer conn.Close()

	// Create a temporary CSV file for testing
	tmpFile, err := os.CreateTemp("", "test_ingest_*.csv")
	suite.Require().NoError(err)
	defer os.Remove(tmpFile.Name())

	// Write test data
	_, err = tmpFile.WriteString("id,name\n1,test1\n2,test2\n")
	suite.Require().NoError(err)
	tmpFile.Close()

	// Test cases
	tests := []struct {
		name          string
		setup         func(stmt adbc.Statement) error
		expectedError string
	}{
		{
			name: "basic ingest with default settings",
			setup: func(stmt adbc.Statement) error {
				err := stmt.SetOption(driver.OptionStringIngestPath, tmpFile.Name())
				if err != nil {
					return err
				}
				return stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreateAppend)
			},
		},
		{
			name: "ingest with custom delimiter",
			setup: func(stmt adbc.Statement) error {
				err := stmt.SetOption(driver.OptionStringIngestPath, tmpFile.Name())
				if err != nil {
					return err
				}
				err = stmt.SetOption(driver.OptionStringIngestFileDelimiter, ";")
				if err != nil {
					return err
				}
				return stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreateAppend)
			},
		},
		{
			name: "ingest with replace mode",
			setup: func(stmt adbc.Statement) error {
				err := stmt.SetOption(driver.OptionStringIngestPath, tmpFile.Name())
				if err != nil {
					return err
				}
				return stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace)
			},
		},
		{
			name: "ingest without file path",
			setup: func(stmt adbc.Statement) error {
				return stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreateAppend)
			},
			expectedError: "cannot execute ingest without a file path",
		},
		{
			name: "ingest with invalid mode",
			setup: func(stmt adbc.Statement) error {
				err := stmt.SetOption(driver.OptionStringIngestPath, tmpFile.Name())
				if err != nil {
					return err
				}
				return stmt.SetOption(adbc.OptionKeyIngestMode, "invalid_mode")
			},
			expectedError: "unsupported ingest mode",
		},
	}

	for _, tt := range tests {
		suite.Run(tt.name, func() {
			stmt, err := conn.NewStatement()
			suite.Require().NoError(err)
			defer stmt.Close()

			// Set destination table
			err = stmt.SetOption(driver.OptionStringQueryDestinationTable, "test_ingest")
			suite.Require().NoError(err)

			// Run setup
			err = tt.setup(stmt)
			suite.Require().NoError(err)

			// Execute query
			reader, affected, err := stmt.ExecuteQuery(ctx)

			if tt.expectedError != "" {
				suite.Error(err)
				suite.Contains(err.Error(), tt.expectedError)
				return
			}

			suite.Require().NoError(err)
			suite.Require().NotNil(reader)
			suite.Equal(int64(0), affected) // Ingest operations return 0 affected rows

			// Verify empty result set
			suite.False(reader.Next())
			suite.NoError(reader.Err())
			reader.Release()
		})
	}
}

var _ validation.DriverQuirks = (*BigQueryQuirks)(nil)
