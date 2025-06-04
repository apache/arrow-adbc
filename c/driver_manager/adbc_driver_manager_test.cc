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

#include <algorithm>
#include <filesystem>  // NOLINT [build/c++17]
#include <memory>
#include <string>
#include <vector>

#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

std::string InternalAdbcDriverManagerDefaultEntrypoint(const std::string& filename);
std::vector<std::filesystem::path> InternalAdbcParsePath(const std::string_view& path);

// Tests of the SQLite example driver, except using the driver manager

namespace adbc {

using adbc_validation::Handle;
using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;

class DriverManager : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

    ASSERT_THAT(AdbcLoadDriver("adbc_driver_sqlite", nullptr, ADBC_VERSION_1_1_0, &driver,
                               &error),
                IsOkStatus(&error));
  }

  void TearDown() override {
    if (error.release) {
      error.release(&error);
    }

    if (driver.release) {
      ASSERT_THAT(driver.release(&driver, &error), IsOkStatus(&error));
      ASSERT_EQ(driver.private_data, nullptr);
      ASSERT_EQ(driver.private_manager, nullptr);
    }
  }

 protected:
  struct AdbcDriver driver = {};
  struct AdbcError error = {};
};

TEST_F(DriverManager, DatabaseCustomInitFunc) {
  struct AdbcDatabase database;
  std::memset(&database, 0, sizeof(database));

  // Explicitly set entrypoint
  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database, "entrypoint", "AdbcDriverInit", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));

  // Set invalid entrypoint
  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database, "entrypoint", "ThisSymbolDoesNotExist", &error),
      IsOkStatus(&error));
  ASSERT_EQ(ADBC_STATUS_INTERNAL, AdbcDatabaseInit(&database, &error));
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
}

TEST_F(DriverManager, ConnectionOptions) {
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  std::memset(&database, 0, sizeof(database));
  std::memset(&connection, 0, sizeof(connection));

  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection, "foo", "bar", &error),
              IsOkStatus(&error));
  ASSERT_EQ(ADBC_STATUS_NOT_IMPLEMENTED,
            AdbcConnectionInit(&connection, &database, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown connection option foo='bar'"));

  ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
}

TEST_F(DriverManager, MultiDriverTest) {
  // Make sure two distinct drivers work in the same process (basic smoke test)
  adbc_validation::Handle<struct AdbcError> error;
  adbc_validation::Handle<struct AdbcDatabase> sqlite_db;
  adbc_validation::Handle<struct AdbcDatabase> postgres_db;
  adbc_validation::Handle<struct AdbcConnection> sqlite_conn;
  adbc_validation::Handle<struct AdbcConnection> postgres_conn;

  ASSERT_THAT(AdbcDatabaseNew(&sqlite_db.value, &error.value), IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseNew(&postgres_db.value, &error.value),
              IsOkStatus(&error.value));

  ASSERT_THAT(AdbcDatabaseSetOption(&sqlite_db.value, "driver", "adbc_driver_sqlite",
                                    &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseSetOption(&postgres_db.value, "driver",
                                    "adbc_driver_postgresql", &error.value),
              IsOkStatus(&error.value));

  ASSERT_THAT(AdbcDatabaseInit(&sqlite_db.value, &error.value), IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseInit(&postgres_db.value, &error.value),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error.value));
  ASSERT_THAT(error->message,
              ::testing::HasSubstr(
                  "[libpq] Must set database option 'uri' before creating a connection"));
  error->release(&error.value);

  ASSERT_THAT(AdbcDatabaseSetOption(&postgres_db.value, "uri",
                                    "postgresql://localhost:5432", &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseSetOption(&sqlite_db.value, "unknown", "foo", &error.value),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error.value));
  ASSERT_THAT(error->message,
              ::testing::HasSubstr("Unknown database option unknown='foo'"));
  error->release(&error.value);

  ASSERT_THAT(AdbcConnectionNew(&sqlite_conn.value, &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcConnectionNew(&postgres_conn.value, &error.value),
              IsOkStatus(&error.value));

  ASSERT_THAT(AdbcConnectionInit(&sqlite_conn.value, &sqlite_db.value, &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcConnectionInit(&postgres_conn.value, &postgres_db.value, &error.value),
              IsStatus(ADBC_STATUS_IO, &error.value));
  ASSERT_THAT(error->message, ::testing::HasSubstr("[libpq] Failed to connect"));
  error->release(&error.value);
}

class SqliteQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    if (AdbcStatusCode res =
            AdbcDatabaseSetOption(database, "driver", "adbc_driver_sqlite", error);
        res != 0) {
      return res;
    }
    return AdbcDatabaseSetOption(
        database, "uri", "file:Sqlite_Transactions?mode=memory&cache=shared", error);
  }

  std::string BindParameter(int index) const override { return "?"; }

  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_BOOL:
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_INT64:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_UINT64:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_HALF_FLOAT:
      case NANOARROW_TYPE_FLOAT:
        return NANOARROW_TYPE_DOUBLE;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return NANOARROW_TYPE_STRING;
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW:
        return NANOARROW_TYPE_BINARY;
      case NANOARROW_TYPE_DATE32:
      case NANOARROW_TYPE_TIMESTAMP:
        return NANOARROW_TYPE_STRING;
      default:
        return ingest_type;
    }
  }

  bool supports_bulk_ingest(const char* mode) const override {
    return std::strcmp(mode, ADBC_INGEST_OPTION_MODE_APPEND) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE) == 0;
  }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_get_option() const override { return false; }
  std::optional<adbc_validation::SqlInfoValue> supports_get_sql_info(
      uint32_t info_code) const override {
    switch (info_code) {
      case ADBC_INFO_DRIVER_NAME:
        return "ADBC SQLite Driver";
      case ADBC_INFO_DRIVER_VERSION:
        return "(unknown)";
      case ADBC_INFO_VENDOR_NAME:
        return "SQLite";
      case ADBC_INFO_VENDOR_VERSION:
        return "3.";
      default:
        return std::nullopt;
    }
  }
  bool supports_metadata_current_catalog() const override { return true; }
  std::string catalog() const override { return "main"; }
};

class SqliteDatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_DATABASE(SqliteDatabaseTest)

TEST_F(SqliteDatabaseTest, NullError) {
  Handle<AdbcConnection> conn;

  ASSERT_THAT(AdbcDatabaseNew(&database, nullptr), IsOkStatus());
  ASSERT_THAT(quirks()->SetupDatabase(&database, nullptr), IsOkStatus());
  ASSERT_THAT(AdbcDatabaseInit(&database, nullptr), IsOkStatus());

  ASSERT_THAT(AdbcConnectionNew(&conn.value, nullptr), IsOkStatus());
  ASSERT_THAT(AdbcConnectionInit(&conn.value, &database, nullptr), IsOkStatus());
  ASSERT_THAT(AdbcConnectionRelease(&conn.value, nullptr), IsOkStatus());

  ASSERT_THAT(AdbcDatabaseRelease(&database, nullptr), IsOkStatus());
}

class SqliteConnectionTest : public ::testing::Test,
                             public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_CONNECTION(SqliteConnectionTest)

class SqliteStatementTest : public ::testing::Test,
                            public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestSqlIngestUInt64() { GTEST_SKIP() << "Cannot ingest UINT64 (out of range)"; }
  void TestSqlIngestTimestamp() {
    GTEST_SKIP() << "Cannot ingest TIMESTAMP (not implemented)";
  }
  void TestSqlIngestTimestampTz() {
    GTEST_SKIP() << "Cannot ingest TIMESTAMP WITH TIMEZONE (not implemented)";
  }
  void TestSqlIngestDuration() {
    GTEST_SKIP() << "Cannot ingest DURATION (not implemented)";
  }
  void TestSqlIngestInterval() {
    GTEST_SKIP() << "Cannot ingest Interval (not implemented)";
  }
  void TestSqlIngestListOfInt32() {
    GTEST_SKIP() << "Cannot ingest list<int32> (not implemented)";
  }
  void TestSqlIngestListOfString() {
    GTEST_SKIP() << "Cannot ingest list<string> (not implemented)";
  }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SqliteStatementTest)

TEST(AdbcDriverManagerInternal, InternalAdbcDriverManagerDefaultEntrypoint) {
  for (const auto& driver : {
           "adbc_driver_sqlite",
           "adbc_driver_sqlite.dll",
           "driver_sqlite",
           "libadbc_driver_sqlite",
           "libadbc_driver_sqlite.so",
           "libadbc_driver_sqlite.so.6.0.0",
           "/usr/lib/libadbc_driver_sqlite.so",
           "/usr/lib/libadbc_driver_sqlite.so.6.0.0",
           "C:\\System32\\adbc_driver_sqlite.dll",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcDriverSqliteInit",
              ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }

  for (const auto& driver : {
           "adbc_sqlite",
           "sqlite",
           "/usr/lib/sqlite.so",
           "C:\\System32\\sqlite.dll",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcSqliteInit", ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }

  for (const auto& driver : {
           "proprietary_engine",
           "libproprietary_engine.so.6.0.0",
           "/usr/lib/proprietary_engine.so",
           "C:\\System32\\proprietary_engine.dll",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcProprietaryEngineInit",
              ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }
}

TEST(AdbcDriverManagerInternal, InternalAdbcParsePath) {
  // Test parsing a path of directories
#ifdef __WIN32
  static const char* const delimiter = ";";
#else
  static const char* const delimiter = ":";
#endif

  std::vector<std::string> paths = {
      "/usr/lib/adbc/drivers", "/usr/local/lib/adbc/drivers",
      "/opt/adbc/drivers",     "/home/user/.config/adbc/drivers",
      "/home/\":foo:\"/bar",
  };

  std::ostringstream joined;
  std::copy(paths.begin(), paths.end(),
            std::ostream_iterator<std::string>(joined, delimiter));

  auto output = InternalAdbcParsePath(joined.str());
  EXPECT_EQ(output.size(), paths.size());

  for (size_t i = 0; i < paths.size(); ++i) {
    EXPECT_EQ(output[i].string(), paths[i]);
  }
}

}  // namespace adbc
