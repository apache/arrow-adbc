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

#include <chrono>
#include <optional>
#include <random>
#include <thread>

#include <adbc.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkErrno;
using adbc_validation::IsOkStatus;

#define CHECK_OK(EXPR)                                              \
  do {                                                              \
    if (auto adbc_status = (EXPR); adbc_status != ADBC_STATUS_OK) { \
      return adbc_status;                                           \
    }                                                               \
  } while (false)

class SqliteFlightSqlQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    const char* uri = std::getenv("ADBC_SQLITE_FLIGHTSQL_URI");
    if (!uri || std::strlen(uri) == 0) {
      ADD_FAILURE() << "Must set ADBC_SQLITE_FLIGHTSQL_URI";
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    EXPECT_THAT(AdbcDatabaseSetOption(database, "uri", uri, error), IsOkStatus(error));
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

  std::string BindParameter(int index) const override { return "?"; }

  bool supports_bulk_ingest(const char* /*mode*/) const override { return false; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return false; }
  bool supports_get_sql_info() const override { return true; }
  std::optional<adbc_validation::SqlInfoValue> supports_get_sql_info(
      uint32_t info_code) const override {
    switch (info_code) {
      case ADBC_INFO_DRIVER_NAME:
        return "ADBC Flight SQL Driver - Go";
      case ADBC_INFO_DRIVER_VERSION:
        return "(unknown or development build)";
      case ADBC_INFO_DRIVER_ADBC_VERSION:
        return ADBC_VERSION_1_1_0;
      case ADBC_INFO_VENDOR_NAME:
        return "db_name";
      case ADBC_INFO_VENDOR_VERSION:
        return "sqlite 3";
      case ADBC_INFO_VENDOR_ARROW_VERSION:
        return "12.0.0";
      default:
        return std::nullopt;
    }
  }
  bool supports_get_objects() const override { return true; }
  bool supports_partitioned_data() const override { return true; }
  bool supports_dynamic_parameter_binding() const override { return true; }
};

class SqliteFlightSqlTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteFlightSqlQuirks quirks_;
};
ADBCV_TEST_DATABASE(SqliteFlightSqlTest)

TEST_F(SqliteFlightSqlTest, TestGarbageInput) {
  // Regression test for https://github.com/apache/arrow-adbc/issues/729

  // 0xc000000000 is the base of the Go heap.  Go's write barriers ask
  // the GC to mark both the pointer being written, and the pointer
  // being *overwritten*.  So if Go overwrites a value in a C
  // structure that looks like a Go pointer, the GC may get confused
  // and error.
  void* bad_pointer = reinterpret_cast<void*>(uintptr_t(0xc000000240));

  // ADBC functions are expected not to blindly overwrite an
  // already-allocated value/callers are expected to zero-initialize.
  database.private_data = bad_pointer;
  database.private_driver = reinterpret_cast<struct AdbcDriver*>(bad_pointer);
  ASSERT_THAT(AdbcDatabaseNew(&database, &error), ::testing::Not(IsOkStatus(&error)));

  std::memset(&database, 0, sizeof(database));
  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->SetupDatabase(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));

  struct AdbcConnection connection;
  connection.private_data = bad_pointer;
  connection.private_driver = reinterpret_cast<struct AdbcDriver*>(bad_pointer);
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), ::testing::Not(IsOkStatus(&error)));

  std::memset(&connection, 0, sizeof(connection));
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  struct AdbcStatement statement;
  statement.private_data = bad_pointer;
  statement.private_driver = reinterpret_cast<struct AdbcDriver*>(bad_pointer);
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error),
              ::testing::Not(IsOkStatus(&error)));

  // This needs to happen in parallel since we need to trigger the
  // write barrier buffer, which means we need to trigger a GC.  The
  // Go FFI bridge deterministically triggers GC on Release calls.

  auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    std::vector<std::thread> threads;
    std::random_device rd;
    for (int i = 0; i < 23; i++) {
      auto seed = rd();
      threads.emplace_back([&, seed]() {
        std::mt19937 gen(seed);
        std::uniform_int_distribution<int64_t> dist(0xc000000000L, 0xc000002000L);
        for (int i = 0; i < 23; i++) {
          void* bad_pointer = reinterpret_cast<void*>(uintptr_t(dist(gen)));

          struct AdbcStatement statement;
          std::memset(&statement, 0, sizeof(statement));
          ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error),
                      IsOkStatus(&error));

          ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error),
                      IsOkStatus(&error));
          // This is not expected to be zero-initialized
          struct ArrowArrayStream stream;
          stream.private_data = bad_pointer;
          stream.release =
              reinterpret_cast<void (*)(struct ArrowArrayStream*)>(bad_pointer);
          ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &stream, nullptr, &error),
                      IsOkStatus(&error));

          struct ArrowSchema schema;
          std::memset(&schema, 0, sizeof(schema));
          schema.name = reinterpret_cast<const char*>(bad_pointer);
          schema.format = reinterpret_cast<const char*>(bad_pointer);
          schema.private_data = bad_pointer;
          ASSERT_THAT(stream.get_schema(&stream, &schema), IsOkErrno());

          while (true) {
            struct ArrowArray array;
            array.private_data = bad_pointer;
            ASSERT_THAT(stream.get_next(&stream, &array), IsOkErrno());
            if (array.release) {
              array.release(&array);
            } else {
              break;
            }
          }

          schema.release(&schema);
          stream.release(&stream);
          ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
        }
      });
    }
    for (auto& thread : threads) {
      thread.join();
    }
  }

  ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
}

class SqliteFlightSqlConnectionTest : public ::testing::Test,
                                      public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteFlightSqlQuirks quirks_;
};
ADBCV_TEST_CONNECTION(SqliteFlightSqlConnectionTest)

class SqliteFlightSqlStatementTest : public ::testing::Test,
                                     public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestSqlIngestTableEscaping() { GTEST_SKIP() << "Table escaping not implemented"; }

 protected:
  SqliteFlightSqlQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SqliteFlightSqlStatementTest)
