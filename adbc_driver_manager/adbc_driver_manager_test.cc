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
#include "adbc_driver_manager.h"
#include "drivers/test_util.h"

// Tests of the SQLite example driver, except using the driver manager

namespace adbc {

using arrow::PointeesEqual;

class DriverManager : public ::testing::Test {
 public:
  void SetUp() override {
    size_t initialized = 0;
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcLoadDriver("libadbc_driver_sqlite.so", "AdbcSqliteDriverInit",
                              ADBC_VERSION_0_0_1, &driver, &initialized, &error));
    ASSERT_EQ(initialized, ADBC_VERSION_0_0_1);

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseNew(&database, &error));
    ASSERT_NE(database.private_data, nullptr);
    ADBC_ASSERT_OK_WITH_ERROR(
        error,
        AdbcDatabaseSetOption(&database, "driver", "libadbc_driver_sqlite.so", &error));
    ADBC_ASSERT_OK_WITH_ERROR(
        error,
        AdbcDatabaseSetOption(&database, "entrypoint", "AdbcSqliteDriverInit", &error));
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
  AdbcDriver driver;
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};
};

TEST_F(DriverManager, SqlExecute) {
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

TEST_F(DriverManager, SqlExecuteInvalid) {
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

TEST_F(DriverManager, SqlPrepare) {
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

TEST_F(DriverManager, SqlPrepareMultipleParams) {
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

TEST_F(DriverManager, BulkIngestStream) {
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
        error, AdbcStatementSetOption(&statement, "table", "bulk_insert", &error));
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

}  // namespace adbc
