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

#include <algorithm>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

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

class BigQueryQuirks : public adbc_validation::DriverQuirks {
 public:
  BigQueryQuirks() {
    auth_value_ = std::getenv("BIGQUERY_JSON_CREDENTIAL_STRING");
    if (auth_value_ == nullptr || std::strlen(auth_value_) == 0) {
      auth_value_ = std::getenv("BIGQUERY_JSON_CREDENTIAL_FILE");
      if (auth_value_ == nullptr || std::strlen(auth_value_) == 0) {
        skip_ = true;
      } else {
        auth_type_ = "adbc.bigquery.sql.auth_type.json_credential_file";
      }
    } else {
      auth_type_ = "adbc.bigquery.sql.auth_type.json_credential_string";
    }

    catalog_name_ = std::getenv("BIGQUERY_PROJECT_ID");
    if (catalog_name_ == nullptr || std::strlen(catalog_name_) == 0) {
      skip_ = true;
    }
  }

  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    EXPECT_THAT(
        AdbcDatabaseSetOption(database, "adbc.bigquery.sql.auth_type", auth_type_, error),
        IsOkStatus(error));
    EXPECT_THAT(AdbcDatabaseSetOption(database, "adbc.bigquery.sql.auth_credentials",
                                      auth_value_, error),
                IsOkStatus(error));
    EXPECT_THAT(AdbcDatabaseSetOption(database, "adbc.bigquery.sql.project_id",
                                      catalog_name_, error),
                IsOkStatus(error));
    EXPECT_THAT(AdbcDatabaseSetOption(database, "adbc.bigquery.sql.dataset_id",
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

    std::string create = "CREATE TABLE `ADBC_TESTING.";
    create += name;
    create += "` (int64s INT, strings TEXT)";
    CHECK_OK(AdbcStatementSetSqlQuery(&statement.value, create.c_str(), error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    // XXX: is there a better way to wait for BigQuery? (Why does 'CREATE
    // TABLE' not wait for commit?)
    std::this_thread::sleep_for(std::chrono::seconds(5));

    std::string insert = "INSERT INTO `ADBC_TESTING.";
    insert += name;
    insert += "` VALUES (42, 'foo'), (-42, NULL), (NULL, '')";
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
  bool supports_transactions() const override { return false; }
  bool supports_get_sql_info() const override { return false; }
  bool supports_get_objects() const override { return false; }
  bool supports_metadata_current_catalog() const override { return false; }
  bool supports_metadata_current_db_schema() const override { return false; }
  bool supports_partitioned_data() const override { return false; }
  bool supports_dynamic_parameter_binding() const override { return true; }
  bool supports_error_on_incompatible_schema() const override { return false; }
  bool ddl_implicit_commit_txn() const override { return true; }
  std::string db_schema() const override { return schema_; }

  const char* auth_type_;
  const char* auth_value_;
  const char* catalog_name_;
  bool skip_{false};
  std::string schema_;
};

class BigQueryTest : public ::testing::Test, public adbc_validation::DatabaseTest {
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
  BigQueryQuirks quirks_;
};
ADBCV_TEST_DATABASE(BigQueryTest)

class BigQueryConnectionTest : public ::testing::Test,
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
  BigQueryQuirks quirks_;
};
ADBCV_TEST_CONNECTION(BigQueryConnectionTest)

class BigQueryStatementTest : public ::testing::Test,
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
    std::string query = "CREATE SCHEMA `";
    query += quirks_.catalog_name_;
    query += "." + schema_name + "`";

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

    std::string query = "DROP SCHEMA `" + std::string(quirks_.catalog_name_) + "." +
                        quirks_.schema_ + "`";

    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));

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

  static BigQueryQuirks quirks_;
};

BigQueryQuirks BigQueryStatementTest::quirks_;
ADBCV_TEST_STATEMENT(BigQueryStatementTest)
