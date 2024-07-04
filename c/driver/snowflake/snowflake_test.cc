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
#include <random>
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkStatus;

#define CHECK_OK(EXPR)                                              \
  do {                                                              \
    if (auto adbc_status = (EXPR); adbc_status != ADBC_STATUS_OK) { \
      return adbc_status;                                           \
    }                                                               \
  } while (false)

namespace {
std::string GetUuid() {
  static std::random_device dev;
  static std::mt19937 rng(dev());

  std::uniform_int_distribution<int> dist(0, 15);

  const char* v = "0123456789ABCDEF";
  const bool dash[] = {0, 0, 0, 0, 1, 0, 1, 0, 1, 0, 1, 0, 0, 0, 0, 0};

  std::string res;
  for (int i = 0; i < 16; i++) {
    if (dash[i]) res += "-";
    res += v[dist(rng)];
    res += v[dist(rng)];
  }
  return res;
}
}  // namespace

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
    EXPECT_THAT(AdbcDatabaseSetOption(
                    database, "adbc.snowflake.sql.client_option.use_high_precision",
                    "false", error),
                IsOkStatus(error));
    EXPECT_THAT(AdbcDatabaseSetOption(database, "adbc.snowflake.sql.schema",
                                      schema_.c_str(), error),
                IsOkStatus(error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    CHECK_OK(AdbcStatementNew(connection, &statement.value, error));

    std::string drop = "DROP TABLE IF EXISTS \"";
    drop += name;
    drop += "\"";
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

    std::string create = "CREATE TABLE \"";
    create += name;
    create += "\" (int64s INT, strings TEXT)";
    CHECK_OK(AdbcStatementSetSqlQuery(&statement.value, create.c_str(), error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));

    std::string insert = "INSERT INTO \"";
    insert += name;
    insert += "\" VALUES (42, 'foo'), (-42, NULL), (NULL, '')";
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
      case NANOARROW_TYPE_STRING:
      case NANOARROW_TYPE_LARGE_STRING:
        return NANOARROW_TYPE_STRING;
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
  bool supports_dynamic_parameter_binding() const override { return true; }
  bool supports_error_on_incompatible_schema() const override { return false; }
  bool ddl_implicit_commit_txn() const override { return true; }
  std::string db_schema() const override { return schema_; }

  const char* uri_;
  bool skip_{false};
  std::string schema_{"ADBC_TESTING"};
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

  // Supported, but we don't validate the values
  void TestMetadataCurrentCatalog() { GTEST_SKIP(); }
  void TestMetadataCurrentDbSchema() { GTEST_SKIP(); }

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
  void TestSqlIngestDuration() { GTEST_SKIP(); }

  void TestSqlIngestColumnEscaping() { GTEST_SKIP(); }

 public:
  // will need to be updated to SetUpTestSuite when gtest is upgraded
  static void SetUpTestCase() {
    if (quirks_.skip_) {
      GTEST_SKIP();
    }

    struct AdbcError error;
    struct AdbcDatabase db;
    struct AdbcConnection connection;
    struct AdbcStatement statement;

    std::memset(&error, 0, sizeof(error));
    std::memset(&db, 0, sizeof(db));
    std::memset(&connection, 0, sizeof(connection));
    std::memset(&statement, 0, sizeof(statement));

    ASSERT_THAT(AdbcDatabaseNew(&db, &error), IsOkStatus(&error));
    ASSERT_THAT(quirks_.SetupDatabase(&db, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&db, &error), IsOkStatus(&error));

    ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionInit(&connection, &db, &error), IsOkStatus(&error));

    std::string schema_name = "ADBC_TESTING_" + GetUuid();
    std::string query =
        "CREATE SCHEMA IDENTIFIER('\"ADBC_TESTING\".\"" + schema_name + "\"')";

    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));

    quirks_.schema_ = schema_name;

    ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseRelease(&db, &error), IsOkStatus(&error));
  }

  // will need to be updated to TearDownTestSuite when gtest is upgraded
  static void TearDownTestCase() {
    if (quirks_.skip_) {
      GTEST_SKIP();
    }

    struct AdbcError error;
    struct AdbcDatabase db;
    struct AdbcConnection connection;
    struct AdbcStatement statement;

    std::memset(&error, 0, sizeof(error));
    std::memset(&db, 0, sizeof(db));
    std::memset(&connection, 0, sizeof(connection));
    std::memset(&statement, 0, sizeof(statement));

    ASSERT_THAT(AdbcDatabaseNew(&db, &error), IsOkStatus(&error));
    ASSERT_THAT(quirks_.SetupDatabase(&db, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&db, &error), IsOkStatus(&error));

    ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionInit(&connection, &db, &error), IsOkStatus(&error));

    std::string query =
        "DROP SCHEMA IDENTIFIER('\"ADBC_TESTING\".\"" + quirks_.schema_ + "\"')";

    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));

    quirks_.schema_ = "ADBC_TESTING";

    ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseRelease(&db, &error), IsOkStatus(&error));
  }

 protected:
  void ValidateIngestedTemporalData(struct ArrowArrayView* values, ArrowType type,
                                    enum ArrowTimeUnit unit,
                                    const char* timezone) override {
    switch (type) {
      case NANOARROW_TYPE_TIMESTAMP: {
        std::vector<std::optional<int64_t>> expected;
        switch (unit) {
          case NANOARROW_TIME_UNIT_SECOND:
            expected = {std::nullopt, -42, 0, 42};
            break;
          case NANOARROW_TIME_UNIT_MILLI:
            expected = {std::nullopt, -42, 0, 42};
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
        break;
      }
      default:
        FAIL() << "ValidateIngestedTemporalData not implemented for type " << type;
    }
  }

  static SnowflakeQuirks quirks_;
};

SnowflakeQuirks SnowflakeStatementTest::quirks_;
ADBCV_TEST_STATEMENT(SnowflakeStatementTest)
