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

#include <string>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/testing/matchers.h>

#include "adbc.h"
#include "drivers/test_util.h"
#include "validation/adbc_validation.h"

// Tests of the SQLite example driver

namespace adbc {

using arrow::PointeesEqual;

using RecordBatchMatcher =
    decltype(::testing::UnorderedPointwise(PointeesEqual(), arrow::RecordBatchVector{}));

RecordBatchMatcher BatchesAre(const std::shared_ptr<arrow::Schema>& schema,
                              const std::vector<std::string>& batch_json) {
  arrow::RecordBatchVector batches;
  for (const std::string& json : batch_json) {
    batches.push_back(adbc::RecordBatchFromJSON(schema, json));
  }
  return ::testing::UnorderedPointwise(PointeesEqual(), std::move(batches));
}

class Sqlite : public ::testing::Test {
 public:
  void SetUp() override {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseNew(&database, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcDatabaseSetOption(&database, "filename", ":memory:", &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseInit(&database, &error));
    ASSERT_NE(database.private_data, nullptr);

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionNew(&connection, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionInit(&connection, &database, &error));
    ASSERT_NE(connection.private_data, nullptr);
  }

  void TearDown() override {
    if (error.message) {
      error.release(&error);
    }

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRelease(&connection, &error));
    ASSERT_EQ(connection.private_data, nullptr);

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseRelease(&database, &error));
    ASSERT_EQ(database.private_data, nullptr);
  }

 protected:
  void IngestSampleTable(struct AdbcConnection* connection) {
    ArrowArray export_table;
    ArrowSchema export_schema;
    auto bulk_table =
        adbc::RecordBatchFromJSON(bulk_schema, R"([[1, "foo"], [2, "bar"]])");
    ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
    ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    int64_t rows_affected = 0;
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementExecuteUpdate(&statement, &rows_affected, &error));
    ASSERT_EQ(bulk_table->num_rows(), rows_affected);
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};

  std::shared_ptr<arrow::Schema> bulk_schema = arrow::schema(
      {arrow::field("ints", arrow::int64()), arrow::field("strs", arrow::utf8())});

  std::shared_ptr<arrow::DataType> column_schema = arrow::struct_({
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
  std::shared_ptr<arrow::DataType> usage_schema = arrow::struct_({
      arrow::field("fk_catalog", arrow::utf8()),
      arrow::field("fk_db_schema", arrow::utf8()),
      arrow::field("fk_table", arrow::utf8()),
      arrow::field("fk_column_name", arrow::utf8()),
  });
  std::shared_ptr<arrow::DataType> constraint_schema = arrow::struct_({
      arrow::field("constraint_name", arrow::utf8()),
      arrow::field("constraint_type", arrow::utf8(), /*nullable=*/false),
      arrow::field("column_names", arrow::list(arrow::utf8()), /*nullable=*/false),
      arrow::field("column_names", arrow::list(usage_schema)),
  });
  std::shared_ptr<arrow::DataType> table_schema = arrow::struct_({
      arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
      arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
      arrow::field("table_columns", arrow::list(column_schema)),
      arrow::field("table_constraints", arrow::list(constraint_schema)),
  });
  std::shared_ptr<arrow::DataType> db_schema_schema = arrow::struct_({
      arrow::field("db_schema_name", arrow::utf8()),
      arrow::field("db_schema_tables", arrow::list(table_schema)),
  });
  std::shared_ptr<arrow::Schema> catalog_schema = arrow::schema({
      arrow::field("catalog_name", arrow::utf8()),
      arrow::field("catalog_db_schemas", arrow::list(db_schema_schema)),
  });
};

TEST_F(Sqlite, SqlExecute) {
  std::string query = "SELECT 1";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));

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

  {
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
    ADBC_ASSERT_OK_WITH_ERROR(error,
                              AdbcStatementExecuteUpdate(&statement, nullptr, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_insert", &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&statement, &schema, &batches));
    ASSERT_SCHEMA_EQ(*schema, *bulk_schema);
    EXPECT_THAT(batches, ::testing::UnorderedPointwise(PointeesEqual(), {bulk_table}));
  }

  // Append (already exists)
  {
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
    ASSERT_EQ(ADBC_STATUS_ALREADY_EXISTS,
              AdbcStatementExecuteUpdate(&statement, nullptr, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  // Append (proper option)
  {
    ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
    ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                      ADBC_INGEST_OPTION_MODE_APPEND, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error,
                              AdbcStatementExecuteUpdate(&statement, nullptr, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));

    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_insert", &error));

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ASSERT_NO_FATAL_FAILURE(ReadStatement(&statement, &schema, &batches));
    ASSERT_SCHEMA_EQ(*schema, *bulk_schema);
    EXPECT_THAT(
        batches,
        ::testing::UnorderedPointwise(
            PointeesEqual(),
            {adbc::RecordBatchFromJSON(
                bulk_schema, R"([[1, "foo"], [2, "bar"], [1, "foo"], [2, "bar"]])")}));
  }

  // Conflict (table exists)
  {
    auto bulk_schema = arrow::schema({arrow::field("ints", arrow::int64())});
    auto bulk_table = adbc::RecordBatchFromJSON(bulk_schema, R"([[1], [2]])");
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
    ASSERT_EQ(ADBC_STATUS_ALREADY_EXISTS,
              AdbcStatementExecuteUpdate(&statement, nullptr, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  // Append (table does not exist)
  {
    ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
    ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "this_table_does_not_exist", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                      ADBC_INGEST_OPTION_MODE_APPEND, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    ASSERT_EQ(ADBC_STATUS_NOT_FOUND,
              AdbcStatementExecuteUpdate(&statement, nullptr, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  // Append (schema conflict)
  {
    auto bulk_schema = arrow::schema({arrow::field("ints", arrow::int64())});
    auto bulk_table = adbc::RecordBatchFromJSON(bulk_schema, R"([[1], [2]])");
    ASSERT_OK(ExportRecordBatch(*bulk_table, &export_table));
    ASSERT_OK(ExportSchema(*bulk_schema, &export_schema));

    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                      "bulk_insert", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                      ADBC_INGEST_OPTION_MODE_APPEND, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementBind(&statement, &export_table, &export_schema, &error));
    ASSERT_EQ(ADBC_STATUS_ALREADY_EXISTS,
              AdbcStatementExecuteUpdate(&statement, nullptr, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
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
    ADBC_ASSERT_OK_WITH_ERROR(error,
                              AdbcStatementExecuteUpdate(&statement, nullptr, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_insert", &error));

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
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionNew(&connection2, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionInit(&connection2, &database, &error));
  ASSERT_NE(connection.private_data, nullptr);

  {
    std::string query = "CREATE TABLE foo (bar INTEGER)";
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));

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

    std::shared_ptr<arrow::Schema> schema;
    arrow::RecordBatchVector batches;
    ReadStatement(&statement, &schema, &batches);
    ASSERT_SCHEMA_EQ(*schema, *arrow::schema({arrow::field("bar", arrow::null())}));
    EXPECT_TRUE(batches.empty());
  }

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRelease(&connection2, &error));
}

TEST_F(Sqlite, MetadataGetInfo) {
  static std::shared_ptr<arrow::Schema> kInfoSchema = arrow::schema({
      arrow::field("info_name", arrow::uint32(), /*nullable=*/false),
      arrow::field(
          "info_value",
          arrow::dense_union({
              arrow::field("string_value", arrow::utf8()),
              arrow::field("bool_value", arrow::boolean()),
              arrow::field("int64_value", arrow::int64()),
              arrow::field("int32_bitmask", arrow::int32()),
              arrow::field("string_list", arrow::list(arrow::utf8())),
              arrow::field("int32_to_int32_list_map",
                           arrow::map(arrow::int32(), arrow::list(arrow::int32()))),
          })),
  });

  struct ArrowArrayStream stream;
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetInfo(&connection, nullptr, 0, &stream, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  ASSERT_SCHEMA_EQ(*schema, *kInfoSchema);
  ASSERT_EQ(1, batches.size());

  std::vector<uint32_t> info = {
      ADBC_INFO_DRIVER_NAME,
      ADBC_INFO_DRIVER_VERSION,
      ADBC_INFO_VENDOR_NAME,
      ADBC_INFO_VENDOR_VERSION,
  };

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionGetInfo(&connection, info.data(),
                                                         info.size(), &stream, &error));
  batches.clear();
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  ASSERT_SCHEMA_EQ(*schema, *kInfoSchema);
  ASSERT_EQ(1, batches.size());
  ASSERT_EQ(4, batches[0]->num_rows());
}

TEST_F(Sqlite, MetadataGetTableTypes) {
  struct ArrowArrayStream stream;
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcConnectionGetTableTypes(&connection, &stream, &error));

  auto expected_schema = arrow::schema({
      arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
  });
  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  ASSERT_SCHEMA_EQ(*schema, *expected_schema);
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       adbc::RecordBatchFromJSON(
                                           expected_schema, R"([["table"], ["view"]])"),
                                   }));
}

TEST_F(Sqlite, MetadataGetObjects) {
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection));

  // Query for catalogs
  struct ArrowArrayStream stream;
  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;

  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_CATALOGS, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches, BatchesAre(catalog_schema, {R"([["main", null]])"}));
  batches.clear();

  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_CATALOGS, "catalog",
                               nullptr, nullptr, nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches, BatchesAre(catalog_schema, {R"([])"}));
  batches.clear();

  // Query for schemas
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr,
                               nullptr, nullptr, nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(
      batches,
      BatchesAre(
          catalog_schema,
          {R"([["main", [{"db_schema_name": null, "db_schema_tables": null}]]])"}));
  batches.clear();

  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr,
                               "schema", nullptr, nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches, BatchesAre(catalog_schema, {R"([["main", []]])"}));
  batches.clear();

  // Query for tables
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches,
              BatchesAre(catalog_schema,
                         {R"([["main", [{"db_schema_name": null, "db_schema_tables": [
  {"table_name": "bulk_insert", "table_type": "table", "table_columns": null, "table_constraints": null}
]}]]])"}));
  batches.clear();

  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
                               "bulk_%", nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches,
              BatchesAre(catalog_schema,
                         {R"([["main", [{"db_schema_name": null, "db_schema_tables": [
  {"table_name": "bulk_insert", "table_type": "table", "table_columns": null, "table_constraints": null}
]}]]])"}));
  batches.clear();

  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_TABLES, nullptr, nullptr,
                               "asdf%", nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(
      batches,
      BatchesAre(catalog_schema,
                 {R"([["main", [{"db_schema_name": null, "db_schema_tables": []}]]])"}));
  batches.clear();

  // Query for table types
  std::vector<const char*> table_types(2);
  table_types[0] = "table";
  table_types[1] = nullptr;
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, table_types.data(), nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches,
              BatchesAre(catalog_schema,
                         {R"([["main", [{"db_schema_name": null, "db_schema_tables": [
  {
    "table_name": "bulk_insert",
    "table_type": "table",
    "table_columns": [
      ["ints", 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
      ["strs", 2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ],
    "table_constraints": []
  }
]}]]])"}));
  batches.clear();

  table_types[0] = "view";
  table_types[1] = nullptr;
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, table_types.data(), nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(
      batches,
      BatchesAre(catalog_schema,
                 {R"([["main", [{"db_schema_name": null, "db_schema_tables": []}]]])"}));
  batches.clear();

  // Query for columns
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches,
              BatchesAre(catalog_schema,
                         {R"([["main", [{"db_schema_name": null, "db_schema_tables": [
  {
    "table_name": "bulk_insert",
    "table_type": "table",
    "table_columns": [
      ["ints", 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
      ["strs", 2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ],
    "table_constraints": []
  }
]}]]])"}));
  batches.clear();

  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr,
                                      nullptr, nullptr, nullptr, "in%", &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches,
              BatchesAre(catalog_schema,
                         {R"([["main", [{"db_schema_name": null, "db_schema_tables": [
  {
    "table_name": "bulk_insert",
    "table_type": "table",
    "table_columns": [
      ["ints", 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ],
    "table_constraints": []
  }
]}]]])"}));
  batches.clear();
}

TEST_F(Sqlite, MetadataGetObjectsColumns) {
  {
    AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));

    ADBC_ASSERT_OK_WITH_ERROR(
        error,
        AdbcStatementSetSqlQuery(
            &statement, "CREATE TABLE parent (a, b, c, PRIMARY KEY(c, b))", &error));
    ADBC_ASSERT_OK_WITH_ERROR(error,
                              AdbcStatementExecuteUpdate(&statement, nullptr, &error));

    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(&statement, "CREATE TABLE other (a)", &error));
    ADBC_ASSERT_OK_WITH_ERROR(error,
                              AdbcStatementExecuteUpdate(&statement, nullptr, &error));

    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementSetSqlQuery(
                   &statement,
                   "CREATE TABLE child (a, b, c, PRIMARY KEY(a), FOREIGN KEY (c, b) "
                   "REFERENCES parent (c, b), FOREIGN KEY (a) REFERENCES other(a))",
                   &error));
    ADBC_ASSERT_OK_WITH_ERROR(error,
                              AdbcStatementExecuteUpdate(&statement, nullptr, &error));

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  struct ArrowArrayStream stream;
  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;

  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &stream, &error));
  ASSERT_NO_FATAL_FAILURE(ReadStream(&stream, &schema, &batches));
  EXPECT_THAT(batches,
              BatchesAre(catalog_schema,
                         {R"([["main", [{"db_schema_name": null, "db_schema_tables": [
  {
    "table_name": "child",
    "table_type": "table",
    "table_columns": [
      ["a", 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
      ["b", 2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
      ["c", 3, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ],
    "table_constraints": [
      [null, "PRIMARY KEY", ["a"], []],
      [null, "FOREIGN KEY", ["a"], [[null, null, "other", "a"]]],
      [null, "FOREIGN KEY", ["c", "b"], [[null, null, "parent", "c"], [null, null, "parent", "b"]]]
    ]
  },
  {
    "table_name": "other",
    "table_type": "table",
    "table_columns": [
      ["a", 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ],
    "table_constraints": []
  },
  {
    "table_name": "parent",
    "table_type": "table",
    "table_columns": [
      ["a", 1, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
      ["b", 2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null],
      ["c", 3, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null]
    ],
    "table_constraints": [
      [null, "PRIMARY KEY", ["c", "b"], []]
    ]
  }
]}]]])"}));
  batches.clear();
}

TEST_F(Sqlite, MetadataGetTableSchema) {
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection));

  ArrowSchema export_schema;
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionGetTableSchema(&connection, /*catalog=*/nullptr,
                                          /*db_schema=*/nullptr, "bulk_insert",
                                          &export_schema, &error));

  ASSERT_OK_AND_ASSIGN(auto schema, arrow::ImportSchema(&export_schema));
  ASSERT_SCHEMA_EQ(*schema, *bulk_schema);
}

TEST_F(Sqlite, Transactions) {
  // For this test, we explicitly want a shared DB
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRelease(&connection, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseRelease(&database, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseNew(&database, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcDatabaseSetOption(&database, "filename",
                            "file:Sqlite_Transactions?mode=memory&cache=shared", &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseInit(&database, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionNew(&connection, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionInit(&connection, &database, &error));

  struct AdbcConnection connection2;
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionNew(&connection2, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionInit(&connection2, &database, &error));
  ASSERT_NE(connection.private_data, nullptr);

  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));

  const char* query = "SELECT * FROM bulk_insert";

  // Invalid option value
  ASSERT_NE(ADBC_STATUS_OK,
            AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    "invalid", &error));

  // Can't call commit/rollback without disabling autocommit
  ASSERT_EQ(ADBC_STATUS_INVALID_STATE, AdbcConnectionCommit(&connection, &error));
  ASSERT_EQ(ADBC_STATUS_INVALID_STATE, AdbcConnectionRollback(&connection, &error));
  error.release(&error);

  // Ensure it's idempotent
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                     ADBC_OPTION_VALUE_ENABLED, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                     ADBC_OPTION_VALUE_ENABLED, &error));

  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                     ADBC_OPTION_VALUE_DISABLED, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionCommit(&connection, &error));

  // Uncommitted change
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection));

  // SQLite prevents us from executing the query
  {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection2, &statement, &error));
    ASSERT_NE(ADBC_STATUS_OK, AdbcStatementSetSqlQuery(&statement, query, &error));
    ASSERT_THAT(error.message, ::testing::HasSubstr("database schema is locked"));
    error.release(&error);
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  // Rollback
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRollback(&connection, &error));

  // Now nothing's visible
  {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection2, &statement, &error));
    ASSERT_NE(ADBC_STATUS_OK, AdbcStatementSetSqlQuery(&statement, query, &error));
    ASSERT_THAT(error.message, ::testing::HasSubstr("no such table"));
    error.release(&error);
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  // Commit, should now be visible on other connection
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionCommit(&connection, &error));

  {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection2, &statement, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementSetSqlQuery(&statement, query, &error));
    struct ArrowArrayStream stream;
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementExecuteQuery(&statement, &stream, nullptr, &error));
    stream.release(&stream);
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
  }

  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRelease(&connection2, &error));
}

TEST_F(Sqlite, ValidationSuite) {
  struct AdbcValidateTestContext ctx;
  std::memset(&ctx, 0, sizeof(ctx));
  AdbcValidateDatabaseNewRelease(&ctx);
  AdbcValidateConnectionNewRelease(&ctx);
  AdbcValidateConnectionAutocommit(&ctx);
  AdbcValidateStatementNewRelease(&ctx);
  AdbcValidateStatementSqlExecute(&ctx);
  AdbcValidateStatementSqlIngest(&ctx);
  AdbcValidateStatementSqlPrepare(&ctx);
  ASSERT_EQ(ctx.failed, 0);
  ASSERT_EQ(ctx.total, ctx.passed);
}

}  // namespace adbc
