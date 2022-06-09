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
#include <cstdlib>

#include <arrow/record_batch.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/testing/matchers.h>
#include <arrow/util/logging.h>

#include "adbc.h"
#include "drivers/test_util.h"

namespace adbc {

using arrow::PointeesEqual;

static std::string kServerEnvVar = "ADBC_FLIGHT_SQL_LOCATION";

class AdbcFlightSqlTest : public ::testing::Test {
 public:
  void SetUp() override {
    if (const char* location = std::getenv(kServerEnvVar.c_str())) {
      ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseNew(&database, &error));
      ADBC_ASSERT_OK_WITH_ERROR(
          error, AdbcDatabaseSetOption(&database, "location", location, &error));
      ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseInit(&database, &error));
      ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionNew(&database, &connection, &error));
      ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionInit(&connection, &error));
    } else {
      FAIL() << "Must provide location of Flight SQL server at " << kServerEnvVar;
    }
  }

  void TearDown() override {
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionRelease(&connection, &error));
    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseRelease(&database, &error));
  }

 protected:
  AdbcDriver driver;
  AdbcDatabase database;
  AdbcConnection connection;
  AdbcError error = {};
};

TEST_F(AdbcFlightSqlTest, Metadata) {
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcConnectionGetTableTypes(&connection, &statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(
      *schema,
      *arrow::schema({arrow::field("table_type", arrow::utf8(), /*nullable=*/false)}));
  EXPECT_THAT(batches, ::testing::UnorderedPointwise(
                           PointeesEqual(),
                           {
                               adbc::RecordBatchFromJSON(schema, R"([["table"]])"),
                           }));
}

TEST_F(AdbcFlightSqlTest, SqlExecute) {
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

TEST_F(AdbcFlightSqlTest, SqlExecuteInvalid) {
  std::string query = "INVALID";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ASSERT_NE(AdbcStatementExecute(&statement, &error), ADBC_STATUS_OK);
  ADBC_ASSERT_ERROR_THAT(error, ::testing::HasSubstr("syntax error"));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));
}

TEST_F(AdbcFlightSqlTest, Partitions) {
  // Serialize the query result handle into a partition so it can be
  // retrieved separately. (With multiple partitions we could
  // distribute them across multiple machines or fetch data in
  // parallel.)
  std::string query = "SELECT 42";
  AdbcStatement statement;
  std::memset(&statement, 0, sizeof(statement));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error,
                            AdbcStatementSetSqlQuery(&statement, query.c_str(), &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementExecute(&statement, &error));

  std::vector<std::vector<uint8_t>> descs;

  while (true) {
    size_t length = 0;
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementGetPartitionDescSize(&statement, &length, &error));
    if (length == 0) break;
    descs.emplace_back(length);
    ADBC_ASSERT_OK_WITH_ERROR(
        error, AdbcStatementGetPartitionDesc(&statement, descs.back().data(), &error));
  }

  ASSERT_EQ(descs.size(), 1);
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(&statement, &error));

  // Reconstruct the partition
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementNew(&connection, &statement, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcConnectionDeserializePartitionDesc(
                                       &connection, descs.back().data(),
                                       descs.back().size(), &statement, &error));

  std::shared_ptr<arrow::Schema> schema;
  arrow::RecordBatchVector batches;
  ReadStatement(&statement, &schema, &batches);
  ASSERT_SCHEMA_EQ(*schema, *arrow::schema({arrow::field("42", arrow::int64())}));
  EXPECT_THAT(batches,
              ::testing::UnorderedPointwise(
                  PointeesEqual(), {
                                       adbc::RecordBatchFromJSON(schema, "[[42]]"),
                                   }));
}

}  // namespace adbc
