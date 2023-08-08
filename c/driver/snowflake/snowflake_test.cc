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

#include <adbc.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>
#include <algorithm>
#include <cstring>
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkStatus;

#define CHECK_OK(EXPR)                                              \
  do {                                                              \
    if (auto adbc_status = (EXPR); adbc_status != ADBC_STATUS_OK) { \
      return adbc_status;                                           \
    }                                                               \
  } while (false)

class SnowflakeQuirks : public adbc_validation::DriverQuirks {
 public:
  SnowflakeQuirks() {
    uri_ = std::getenv("ADBC_SNOWFLAKE_URI");
    if (uri_ == nullptr || std::strlen(uri_) == 0) {
      skip_ = true;
    }
  }

  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    EXPECT_THAT(AdbcDatabaseSetOption(database, "uri", uri_, error), IsOkStatus(error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    CHECK_OK(AdbcStatementNew(connection, &statement.value, error));

    std::string drop = "DROP TABLE IF EXISTS ";
    drop += name;
    CHECK_OK(AdbcStatementSetSqlQuery(&statement.value, drop.c_str(), error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));

    CHECK_OK(AdbcStatementRelease(&statement.value, error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode CreateSampleTable(struct AdbcConnection* connection,
                                   const std::string& name,
                                   struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    CHECK_OK(AdbcStatementNew(connection, &statement.value, error));

    std::string create = "CREATE TABLE ";
    create += name;
    create += " (int64s INT, strings TEXT)";
    CHECK_OK(AdbcStatementSetSqlQuery(&statement.value, create.c_str(), error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));

    std::string insert = "INSERT INTO ";
    insert += name;
    insert += " VALUES (42, 'foo'), (-42, NULL), (NULL, '')";
    CHECK_OK(AdbcStatementSetSqlQuery(&statement.value, insert.c_str(), error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));

    CHECK_OK(AdbcStatementRelease(&statement.value, error));
    return ADBC_STATUS_OK;
  }

  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_INT64:
      case NANOARROW_TYPE_UINT64:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_FLOAT:
      case NANOARROW_TYPE_DOUBLE:
        return NANOARROW_TYPE_DOUBLE;
      default:
        return ingest_type;
    }
  }

  std::string BindParameter(int index) const override { return "?"; }
  bool supports_bulk_ingest(const char* /*mode*/) const override { return true; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return true; }
  bool supports_get_sql_info() const override { return false; }
  bool supports_get_objects() const override { return true; }
  bool supports_metadata_current_catalog() const override { return false; }
  bool supports_metadata_current_db_schema() const override { return false; }
  bool supports_partitioned_data() const override { return false; }
  bool supports_dynamic_parameter_binding() const override { return false; }
  bool ddl_implicit_commit_txn() const override { return true; }
  std::string db_schema() const override { return "ADBC_TESTING"; }

  const char* uri_;
  bool skip_{false};
};

class SnowflakeTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (quirks_.skip_) {
      GTEST_SKIP();
    }
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    if (!quirks_.skip_) {
      ASSERT_NO_FATAL_FAILURE(TearDownTest());
    }
  }

 protected:
  SnowflakeQuirks quirks_;
};
ADBCV_TEST_DATABASE(SnowflakeTest)

class SnowflakeConnectionTest : public ::testing::Test,
                                public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (quirks_.skip_) {
      GTEST_SKIP();
    }
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    if (!quirks_.skip_) {
      ASSERT_NO_FATAL_FAILURE(TearDownTest());
    }
  }

 protected:
  SnowflakeQuirks quirks_;
};
ADBCV_TEST_CONNECTION(SnowflakeConnectionTest)

class SnowflakeStatementTest : public ::testing::Test,
                               public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (quirks_.skip_) {
      GTEST_SKIP();
    }
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    if (!quirks_.skip_) {
      ASSERT_NO_FATAL_FAILURE(TearDownTest());
    }
  }

  void TestSqlIngestInterval() { GTEST_SKIP(); }

 protected:
  void ValidateIngestedTimestampData(struct ArrowArrayView* values,
                                     enum ArrowTimeUnit unit,
                                     const char* timezone) override {
    std::vector<std::optional<int64_t>> expected;
    switch (unit) {
      case NANOARROW_TIME_UNIT_SECOND:
        expected = {std::nullopt, -42, 0, 42};
        break;
      case NANOARROW_TIME_UNIT_MILLI:
        expected = {std::nullopt, -42000, 0, 42000};
        break;
      case NANOARROW_TIME_UNIT_MICRO:
        expected = {std::nullopt, -42, 0, 42};
        break;
      case NANOARROW_TIME_UNIT_NANO:
        expected = {std::nullopt, -42, 0, 42};
        break;
    }
    ASSERT_NO_FATAL_FAILURE(
        adbc_validation::CompareArray<std::int64_t>(values, expected));
  }

  SnowflakeQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SnowflakeStatementTest)
