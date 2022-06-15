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

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/testing/matchers.h>

#include "adbc.h"
#include "drivers/test_util.h"

// Tests of the SQLite example driver

namespace adbc {

using arrow::PointeesEqual;

class Sqlite : public ::testing::Test {
 public:
  void SetUp() override {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseNew(&database, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcDatabaseSetOption(&database, "filename", ":memory:", &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseInit(&database, &error));
    ASSERT_NE(database.private_data, nullptr);

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionNew(&database, &connection, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionInit(&connection, &error));
    ASSERT_NE(connection.private_data, nullptr);
  }

  void TearDown() override {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRelease(&connection, &error));
    ASSERT_EQ(connection.private_data, nullptr);

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseRelease(&database, &error));
    ASSERT_EQ(database.private_data, nullptr);
  }

 protected:
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};
};

TEST_F(Sqlite, SqlExecute) {
  std::string query = "SELECT 1";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *arrow::schema({arrow::field("1", arrow::int64())}));
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       adbc::RecordBatchFromJSON(schema, "[[1]]"),
                                   }));
}

TEST_F(Sqlite, SqlExecuteInvalid) {
  std::string query = "INVALID";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ASSERT_NE(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error), ADBC_STATUS_OK);
  ADBC_ASSERT_ERROR_THAT(
      error, ::testing::AllOf(::testing::HasSubstr("[SQLite3] sqlite3_prepare_v2:"),
                              ::testing::HasSubstr("syntax error")));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
}

TEST_F(Sqlite, SqlPrepare) {
  std::string query = "SELECT 1";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementPrepare(&statement, &error));

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ASSERT_NO_FATAL_FAILURE(ReadStatement(&statement, &schema, &batches));
  ASSERT_SCHEMA_EQ(*schema, *arrow::schema({arrow::field("1", arrow::int64())}));
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       adbc::RecordBatchFromJSON(schema, "[[1]]"),
                                   }));
}

TEST_F(Sqlite, SqlPrepareMultipleParams) {
  auto param_schema = arrow::schema(
      {arrow::field("1", arrow::int64()), arrow::field("2", arrow::utf8())});
  std::string query = "SELECT ?, ?";
  AdbcStatement statement;
  ArrowArray export_params;
  ArrowSchema export_schema;
  std::memset(&statement, 0, sizeof(statement));

  ASSERT_OK(ExportRecordBatch(
      *adbc::RecordBatchFromJSON(param_schema, R"([[1, "foo"], [2, "bar"]])"),
      &export_params));
  ASSERT_OK(ExportSchema(*param_schema, &export_schema));

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementPrepare(&statement, &error));

  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcStatementBind(&statement, &export_params, &export_schema, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ASSERT_NO_FATAL_FAILURE(ReadStatement(&statement, &schema, &batches));
  ASSERT_SCHEMA_EQ(*schema, *arrow::schema({arrow::field("?", arrow::int64()),
                                            arrow::field("?", arrow::utf8())}));
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(),
                  {
                      adbc::RecordBatchFromJSON(schema, R"([[1, "foo"], [2, "bar"]])"),
                  }));
}

TEST_F(Sqlite, BulkIngestTable) {
  ArrowArray export_table;
  ArrowSchema export_schema;
  auto bulk_schema = arrow::schema(
      {arrow::field("ints", arrow::int64()), arrow::field("strs", arrow::utf8())});
  auto bulk_table = adbc::RecordBatchFromJSON(bulk_schema, R"([[1, "foo"], [2, "bar"]])");
  ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
  ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));
  }

  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&statement, &schema, &batches));
    ASSERT_SCHEMA_EQ(*schema, *bulk_schema);
    EXPECT_THAT(batches, ::testing::UnorderedPointwise(PointeesEqual(), {bulk_table}));
  }
}

TEST_F(Sqlite, BulkIngestStream) {
  ArrowArrayStream export_stream;
  auto bulk_schema = arrow::schema(
      {arrow::field("ints", arrow::int64()), arrow::field("strs", arrow::utf8())});
  std::vector<std::shared_ptr<arrow::RecordBatch>> bulk_batches{
      adbc::RecordBatchFromJSON(bulk_schema, R"([[1, "foo"], [2, "bar"]])"),
      adbc::RecordBatchFromJSON(bulk_schema, R"([[3, ""], [4, "baz"]])"),
  };
  auto bulk_table = *arrow::Table::FromRecordBatches(bulk_batches);
  auto reader = std::make_shared<arrow::TableBatchReader>(*bulk_table);
  ASSERT_OK(arrow::ExportRecordBatchReader(reader, &export_stream));

  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBindStream(&statement, &export_stream, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));
  }

  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&statement, &schema, &batches));
    ASSERT_SCHEMA_EQ(*schema, *bulk_schema);
    EXPECT_THAT(
        batches,
        ::testing::UnorderedPointwise(
            PointeesEqual(),
            {
                adbc::RecordBatchFromJSON(
                    bulk_schema, R"([[1, "foo"], [2, "bar"], [3, ""], [4, "baz"]])"),
            }));
  }
}

TEST_F(Sqlite, MultipleConnections) {
  struct AdbcConnection connection2;

  {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionNew(&database, &connection2, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionInit(&connection2, &error));
    ASSERT_NE(connection.private_data, nullptr);
  }

  {
    std::string query = "CREATE TABLE foo (bar INTEGER)";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&statement, &schema, &batches));
    ASSERT_SCHEMA_EQ(*schema, *arrow::schema({}));
    EXPECT_TRUE(batches.empty());
  }

  {
    std::string query = "SELECT * FROM foo";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&statement, &schema, &batches);
    ASSERT_SCHEMA_EQ(*schema, *arrow::schema({arrow::field("bar", arrow::null())}));
    EXPECT_TRUE(batches.empty());
  }

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRelease(&connection2, &error));
}

TEST_F(Sqlite, MetadataGetColumns) {
  // Create a table via ingestion
  {
    ArrowArray export_table;
    ArrowSchema export_schema;
    auto bulk_schema = arrow::schema(
        {arrow::field("ints", arrow::int64()), arrow::field("strs", arrow::utf8())});
    auto bulk_table =
        adbc::RecordBatchFromJSON(bulk_schema, R"([[1, "foo"], [2, "bar"]])");
    ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
    ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));
  }

  auto expected_schema = arrow::schema({
      arrow::field("catalog_name", arrow::utf8()),
      arrow::field("db_schema_name", arrow::utf8()),
      arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
      arrow::field("column_name", arrow::utf8(), /*nullable=*/false),
      arrow::field("ordinal_position", arrow::int32()),
      arrow::field("remarks", arrow::utf8()),
      arrow::field("xdbc_data_type", arrow::int16()),
      arrow::field("xdbc_type_name", arrow::utf8()),
      arrow::field("xdbc_column_size", arrow::int32()),
      arrow::field("xdbc_decimal_digits", arrow::int16()),
      arrow::field("xdbc_num_prec_radix", arrow::int16()),
      arrow::field("xdbc_nullable", arrow::int16()),
      arrow::field("xdbc_column_def", arrow::utf8()),
      arrow::field("xdbc_sql_data_type", arrow::int16()),
      arrow::field("xdbc_datetime_sub", arrow::int16()),
      arrow::field("xdbc_char_octet_length", arrow::int32()),
      arrow::field("xdbc_is_nullable", arrow::utf8()),
      arrow::field("xdbc_scope_catalog", arrow::utf8()),
      arrow::field("xdbc_scope_schema", arrow::utf8()),
      arrow::field("xdbc_scope_table", arrow::utf8()),
      arrow::field("xdbc_is_autoincrement", arrow::boolean()),
      arrow::field("xdbc_is_generatedcolumn", arrow::boolean()),
  });

  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetColumns(&connection, /*catalog=*/nullptr,
                                      /*db_schema=*/nullptr, /*table_name=*/nullptr,
                                      /*column_name=*/nullptr, &statement, &error));
  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       adbc::RecordBatchFromJSON(expected_schema, R"([
    [null, null, "bulk_insert", "ints", 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
    [null, null, "bulk_insert", "strs", 2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
])"),
                                   }));
}

TEST_F(Sqlite, MetadataGetCatalogs) {
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcConnectionGetCatalogs(&connection, &statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(
      *schema,
      *arrow::schema({arrow::field("catalog_name", arrow::utf8(), /*nullable=*/false)}));
  EXPECT_TRUE(batches.empty());
}

TEST_F(Sqlite, MetadataGetDbSchemas) {
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcConnectionGetDbSchemas(&connection, &statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema,
                   *arrow::schema({
                       arrow::field("catalog_name", arrow::utf8()),
                       arrow::field("db_schema_name", arrow::utf8(), /*nullable=*/false),
                   }));
  EXPECT_TRUE(batches.empty());
}

TEST_F(Sqlite, MetadataGetTableTypes) {
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcConnectionGetTableTypes(&connection, &statement, &error));

  auto expected_schema = arrow::schema({
      arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
  });
  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       adbc::RecordBatchFromJSON(
                                           expected_schema, R"([["table"], ["view"]])"),
                                   }));
}

TEST_F(Sqlite, MetadataGetTables) {
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetTables(&connection, /*catalog=*/nullptr,
                                     /*db_schema=*/nullptr, /*table_name=*/nullptr,
                                     /*table_types=*/nullptr, &statement, &error));

  auto expected_schema = arrow::schema({
      arrow::field("catalog_name", arrow::utf8()),
      arrow::field("db_schema_name", arrow::utf8()),
      arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
      arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
  });
  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;

  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_TRUE(batches.empty());
  batches.clear();

  // Create a table via ingestion
  {
    ArrowArray export_table;
    ArrowSchema export_schema;
    auto bulk_schema = arrow::schema(
        {arrow::field("ints", arrow::int64()), arrow::field("strs", arrow::utf8())});
    auto bulk_table =
        adbc::RecordBatchFromJSON(bulk_schema, R"([[1, "foo"], [2, "bar"]])");
    ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
    ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));
  }

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetTables(&connection, "", "", /*table_name=*/nullptr,
                                     /*table_types=*/nullptr, &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(),
                  {
                      adbc::RecordBatchFromJSON(
                          expected_schema, R"([[null, null, "bulk_insert", "table"]])"),
                  }));
  batches.clear();

  // Table name filter
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetTables(&connection, /*catalog=*/nullptr, /*db_schema=*/nullptr,
                              "foo%", /*table_types=*/nullptr, &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_TRUE(batches.empty());
  batches.clear();

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetTables(&connection, /*catalog=*/nullptr, /*db_schema=*/nullptr,
                              "bulk%", /*table_types=*/nullptr, &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(),
                  {
                      adbc::RecordBatchFromJSON(
                          expected_schema, R"([[null, null, "bulk_insert", "table"]])"),
                  }));
  batches.clear();

  // Table type filter
  std::vector<const char*> types(1);
  types[0] = nullptr;
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetTables(&connection, /*catalog=*/nullptr, /*db_schema=*/nullptr,
                              /*table_name=*/nullptr, types.data(), &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(),
                  {
                      adbc::RecordBatchFromJSON(
                          expected_schema, R"([[null, null, "bulk_insert", "table"]])"),
                  }));
  batches.clear();

  types.resize(2);
  types[0] = "view";
  types[1] = nullptr;
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetTables(&connection, /*catalog=*/nullptr, /*db_schema=*/nullptr,
                              /*table_name=*/nullptr, types.data(), &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_TRUE(batches.empty());
  batches.clear();

  types.resize(3);
  types[0] = "view";
  types[1] = "table";
  types[2] = nullptr;
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetTables(&connection, /*catalog=*/nullptr, /*db_schema=*/nullptr,
                              /*table_name=*/nullptr, types.data(), &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(),
                  {
                      adbc::RecordBatchFromJSON(
                          expected_schema, R"([[null, null, "bulk_insert", "table"]])"),
                  }));
  batches.clear();

  // Both
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetTables(&connection, /*catalog=*/nullptr, /*db_schema=*/nullptr,
                              "bulk_%", types.data(), &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(),
                  {
                      adbc::RecordBatchFromJSON(
                          expected_schema, R"([[null, null, "bulk_insert", "table"]])"),
                  }));
  batches.clear();

  // Catalog/schema filter
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetTables(&connection, "foo", "foo", "bulk_%", types.data(),
                                     &statement, &error));
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_TRUE(batches.empty());
  batches.clear();
}

TEST_F(Sqlite, MetadataGetTableSchema) {
  // Create a table via ingestion
  auto bulk_schema = arrow::schema(
      {arrow::field("ints", arrow::int64()), arrow::field("strs", arrow::utf8())});
  {
    ArrowArray export_table;
    ArrowSchema export_schema;
    auto bulk_table =
        adbc::RecordBatchFromJSON(bulk_schema, R"([[1, "foo"], [2, "bar"]])");
    ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
    ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));
  }

  ArrowSchema export_schema;
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetTableSchema(&connection, /*catalog=*/nullptr,
                                          /*db_schema=*/nullptr, "bulk_insert",
                                          &export_schema, &error));

  ASSERT_OK_AND_ASSIGN(auto schema, arrow::ImportSchema(&export_schema));
  ASSERT_SCHEMA_EQ(*schema, *bulk_schema);
}

}  // namespace adbc
