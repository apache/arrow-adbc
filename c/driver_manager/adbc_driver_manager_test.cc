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

#if defined(_WIN32)
#include <windows.h>
#endif

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <filesystem>  // NOLINT [build/c++17]
#include <iostream>
#include <string>
#include <toml++/toml.hpp>
#include <vector>

#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"
#include "current_arch.h"
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

std::string InternalAdbcDriverManagerDefaultEntrypoint(const std::string& filename);
std::vector<std::filesystem::path> InternalAdbcParsePath(const std::string_view path);
std::filesystem::path InternalAdbcUserConfigDir();

struct ParseDriverUriResult {
  std::string_view driver;
  std::optional<std::string_view> uri;
};

std::optional<ParseDriverUriResult> InternalAdbcParseDriverUri(std::string_view str);

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
  adbc_validation::Handle<struct AdbcDatabase> database;

  // Explicitly set entrypoint
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "entrypoint", "AdbcDriverInit", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // Set invalid entrypoint
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "entrypoint",
                                    "ThisSymbolDoesNotExist", &error),
              IsOkStatus(&error));
  ASSERT_EQ(ADBC_STATUS_INTERNAL, AdbcDatabaseInit(&database.value, &error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
}

TEST_F(DriverManager, ConnectionOptions) {
  adbc_validation::Handle<struct AdbcDatabase> database;
  adbc_validation::Handle<struct AdbcConnection> connection;

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection.value, "foo", "bar", &error),
              IsOkStatus(&error));
  ASSERT_EQ(ADBC_STATUS_NOT_IMPLEMENTED,
            AdbcConnectionInit(&connection.value, &database.value, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown connection option foo='bar'"));

  ASSERT_THAT(AdbcConnectionRelease(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
}

TEST_F(DriverManager, GetOptionDatabase) {
  adbc_validation::Handle<struct AdbcDatabase> database;

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "stringoption", "1", &error),
              IsOkStatus(&error));
  std::vector<uint8_t> bytes_value;
  bytes_value.push_back(static_cast<uint8_t>(0));
  bytes_value.push_back(static_cast<uint8_t>(1));
  ASSERT_THAT(AdbcDatabaseSetOptionBytes(&database.value, "bytesoption",
                                         bytes_value.data(), bytes_value.size(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOptionDouble(&database.value, "doubleoption", 42.0, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOptionInt(&database.value, "intoption", 42, &error),
              IsOkStatus(&error));

  {
    double value = 0.0;
    ASSERT_THAT(
        AdbcDatabaseGetOptionDouble(&database.value, "doubleoption", &value, &error),
        IsOkStatus(&error));
    ASSERT_EQ(42.0, value);
  }

  {
    int64_t value = 0;
    ASSERT_THAT(AdbcDatabaseGetOptionInt(&database.value, "intoption", &value, &error),
                IsOkStatus(&error));
    ASSERT_EQ(42, value);
  }

  {
    std::vector<uint8_t> value(32, 42);
    size_t length = value.size();
    ASSERT_THAT(
        AdbcDatabaseGetOptionBytes(&database.value, "bytesoption",
                                   const_cast<uint8_t*>(value.data()), &length, &error),
        IsOkStatus(&error));
    ASSERT_EQ(2, length);
    ASSERT_EQ(0, value[0]);
    ASSERT_EQ(1, value[1]);

    ASSERT_THAT(
        AdbcDatabaseGetOptionBytes(&database.value, "nonexistent",
                                   const_cast<uint8_t*>(value.data()), &length, &error),
        IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }

  {
    std::vector<char> value(32, 'A');
    size_t length = value.size();
    ASSERT_THAT(AdbcDatabaseGetOption(&database.value, "driver",
                                      const_cast<char*>(value.data()), &length, &error),
                IsOkStatus(&error));
    ASSERT_EQ(19, length);
    std::string actual(value.data(), 18);
    std::string expected = "adbc_driver_sqlite";
    ASSERT_EQ(expected, actual);

    ASSERT_THAT(AdbcDatabaseGetOption(&database.value, "stringoption",
                                      const_cast<char*>(value.data()), &length, &error),
                IsOkStatus(&error));
    ASSERT_EQ(2, length);
    actual = std::string(value.data(), 1);
    expected = "1";
    ASSERT_EQ(expected, actual);

    ASSERT_THAT(AdbcDatabaseGetOption(&database.value, "nonexistent",
                                      const_cast<char*>(value.data()), &length, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }
}

TEST_F(DriverManager, GetOptionConnection) {
  adbc_validation::Handle<struct AdbcDatabase> database;
  adbc_validation::Handle<struct AdbcConnection> connection;

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "adbc_driver_sqlite", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionNew(&connection.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection.value, "stringoption", "1", &error),
              IsOkStatus(&error));
  std::vector<uint8_t> bytes_value;
  bytes_value.push_back(static_cast<uint8_t>(0));
  bytes_value.push_back(static_cast<uint8_t>(1));
  ASSERT_THAT(
      AdbcConnectionSetOptionBytes(&connection.value, "bytesoption", bytes_value.data(),
                                   bytes_value.size(), &error),
      IsOkStatus(&error));
  ASSERT_THAT(
      AdbcConnectionSetOptionDouble(&connection.value, "doubleoption", 42.0, &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOptionInt(&connection.value, "intoption", 42, &error),
              IsOkStatus(&error));

  {
    double value = 0.0;
    ASSERT_THAT(
        AdbcConnectionGetOptionDouble(&connection.value, "doubleoption", &value, &error),
        IsOkStatus(&error));
    ASSERT_EQ(42.0, value);
  }

  {
    int64_t value = 0;
    ASSERT_THAT(
        AdbcConnectionGetOptionInt(&connection.value, "intoption", &value, &error),
        IsOkStatus(&error));
    ASSERT_EQ(42, value);
  }

  {
    std::vector<uint8_t> value(32, 42);
    size_t length = value.size();
    ASSERT_THAT(
        AdbcConnectionGetOptionBytes(&connection.value, "bytesoption",
                                     const_cast<uint8_t*>(value.data()), &length, &error),
        IsOkStatus(&error));
    ASSERT_EQ(2, length);
    ASSERT_EQ(0, value[0]);
    ASSERT_EQ(1, value[1]);

    ASSERT_THAT(
        AdbcConnectionGetOptionBytes(&connection.value, "nonexistent",
                                     const_cast<uint8_t*>(value.data()), &length, &error),
        IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }

  {
    std::vector<char> value(32, 'A');
    size_t length = value.size();
    ASSERT_THAT(AdbcConnectionGetOption(&connection.value, "stringoption",
                                        const_cast<char*>(value.data()), &length, &error),
                IsOkStatus(&error));
    ASSERT_EQ(2, length);
    std::string actual(value.data(), 1);
    std::string expected = "1";
    ASSERT_EQ(expected, actual);

    ASSERT_THAT(AdbcConnectionGetOption(&connection.value, "nonexistent",
                                        const_cast<char*>(value.data()), &length, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }
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

TEST_F(DriverManager, NoDefaultEntrypoint) {
#if !defined(ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB)
  GTEST_SKIP() << "ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB is not defined";
#else
  adbc_validation::Handle<struct AdbcError> error;
  adbc_validation::Handle<struct AdbcDriver> driver;
  // Must fail with expected status
  ASSERT_THAT(AdbcLoadDriver(ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB, nullptr,
                             ADBC_VERSION_1_1_0, &driver.value, &error.value),
              IsStatus(ADBC_STATUS_IO, &error.value));

#endif  // !defined(ADBC_DRIVER_MANAGER_ENTRYPOINT_TEST_LIB)
}

TEST_F(DriverManager, NoDefaultEntrypointFound) {
#if !defined(ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB)
  GTEST_SKIP() << "ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB is not defined";
#else
  adbc_validation::Handle<struct AdbcError> error;
  adbc_validation::Handle<struct AdbcDriver> driver;
  ASSERT_THAT(AdbcLoadDriver(ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB, nullptr,
                             ADBC_VERSION_1_1_0, &driver.value, &error.value),
              IsStatus(ADBC_STATUS_INTERNAL, &error.value));
  // Both symbols should not be found, should be mentioned in error message
  ASSERT_THAT(error->message,
              ::testing::HasSubstr("(AdbcDriverNoEntrypointInit) failed"));
  ASSERT_THAT(error->message, ::testing::HasSubstr("(AdbcDriverInit) failed"));

#endif  // !defined(ADBC_DRIVER_MANAGER_NO_ENTRYPOINT_TEST_LIB)
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

// Exercise AdbcDatabaseGetOption with different value buffer lengths
TEST(AdbcDriverManagerInternal, DatabaseGetOptionValueBufferSize) {
  struct AdbcDatabase database = {};
  struct AdbcError error = {};

  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));

  // Set an option we'll later fetch with a n oversized buffer
  ASSERT_THAT(AdbcDatabaseSetOption(&database, "driver", "test_driver", &error),
              IsOkStatus(&error));

  // Too small
  char buf_too_small[11];
  std::memset(buf_too_small, '*',
              sizeof(buf_too_small));  // Pre-fill with "*" just for debugging
  size_t len_too_small = sizeof(buf_too_small);
  ASSERT_THAT(
      AdbcDatabaseGetOption(&database, "driver", buf_too_small, &len_too_small, &error),
      IsOkStatus(&error));
  EXPECT_EQ(len_too_small, 12u);
  EXPECT_STRNE(buf_too_small, "test_driver");

  // Just right
  char buf_just_right[12];
  std::memset(buf_just_right, '*',
              sizeof(buf_just_right));  // Pre-fill with "*" just for debugging
  size_t len_just_right = sizeof(buf_just_right);
  ASSERT_THAT(
      AdbcDatabaseGetOption(&database, "driver", buf_just_right, &len_just_right, &error),
      IsOkStatus(&error));
  EXPECT_EQ(len_just_right, 12u);
  EXPECT_STREQ(buf_just_right, "test_driver");

  // Too large
  char buf_too_large[13];
  std::memset(buf_too_large, '*',
              sizeof(buf_too_large));  // Pre-fill with "*" just for debugging
  size_t len_too_large = sizeof(buf_too_large);
  ASSERT_THAT(
      AdbcDatabaseGetOption(&database, "driver", buf_too_large, &len_too_large, &error),
      IsOkStatus(&error));
  EXPECT_EQ(len_too_large, 12u);
  EXPECT_STREQ(buf_too_large, "test_driver");

  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
}

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

  for (const auto& driver : {
           "driver_example",
           "libdriver_example.so",
       }) {
    SCOPED_TRACE(driver);
    EXPECT_EQ("AdbcDriverExampleInit",
              ::InternalAdbcDriverManagerDefaultEntrypoint(driver));
  }
}

TEST(AdbcDriverManagerInternal, InternalAdbcParsePath) {
  // Test parsing a path of directories
#ifdef _WIN32
  static const char* const delimiter = ";";
#else
  static const char* const delimiter = ":";
#endif

  std::vector<std::string> paths = {
      "/usr/lib/adbc/drivers", "/usr/local/lib/adbc/drivers",
      "/opt/adbc/drivers",     "/home/user/.config/adbc/drivers",
#ifdef _WIN32
      "/home/\":foo:\"/bar",
#endif
  };

  std::ostringstream joined;
  std::copy(paths.begin(), paths.end(),
            std::ostream_iterator<std::string>(joined, delimiter));

  auto output = InternalAdbcParsePath(joined.str());
  EXPECT_THAT(output, ::testing::ElementsAreArray(paths));
}

TEST(AdbcDriverManagerInternal, InternalAdbcParseDriverUri) {
  std::vector<std::pair<std::string, std::optional<ParseDriverUriResult>>> uris = {
      {"sqlite", std::nullopt},
      {"sqlite:", {{"sqlite", std::nullopt}}},
      {"sqlite:file::memory:", {{"sqlite", "file::memory:"}}},
      {"sqlite:file::memory:?cache=shared", {{"sqlite", "file::memory:?cache=shared"}}},
      {"postgresql://a:b@localhost:9999/nonexistent",
       {{"postgresql", "postgresql://a:b@localhost:9999/nonexistent"}}}};

#ifdef _WIN32
  auto temp_dir = std::filesystem::temp_directory_path() / "adbc_driver_manager_tests";
  std::filesystem::create_directories(temp_dir);
  std::string temp_driver_path = temp_dir.string() + "\\driver.dll";
  std::ofstream temp_driver_file(temp_driver_path);
  temp_driver_file << "placeholder";
  temp_driver_file.close();

  uris.push_back({temp_driver_path, {{temp_driver_path, std::nullopt}}});
#endif

  auto cmp = [](std::optional<ParseDriverUriResult> a,
                std::optional<ParseDriverUriResult> b) {
    if (!a.has_value()) {
      EXPECT_FALSE(b.has_value());
      return;
    }
    EXPECT_EQ(a->driver, b->driver);
    if (!a->uri) {
      EXPECT_FALSE(b->uri);
    } else {
      EXPECT_EQ(*a->uri, *b->uri);
    }
  };

  for (const auto& [uri, expected] : uris) {
    std::string_view uri_view = uri;
    auto result = InternalAdbcParseDriverUri(uri_view);
    cmp(result, expected);
  }

#ifdef _WIN32
  std::filesystem::remove_all(temp_dir);
#endif
}

class DriverManifest : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

#ifndef ADBC_DRIVER_MANAGER_TEST_LIB
    GTEST_SKIP() << "ADBC_DRIVER_MANAGER_TEST_LIB is not defined. "
                    "This test requires a driver library to be specified.";
#else
    driver_path = std::filesystem::path(ADBC_DRIVER_MANAGER_TEST_LIB);
    if (!std::filesystem::exists(driver_path)) {
      GTEST_SKIP() << "Driver library does not exist: " << driver_path;
    }

    simple_manifest = toml::table{
        {"name", "SQLite3"},
        {"publisher", "arrow-adbc"},
        {"version", "X.Y.Z"},
        {"ADBC",
         toml::table{
             {"version", "1.1.0"},
         }},
        {"Driver",
         toml::table{
             {"shared",
              toml::table{
                  {adbc::CurrentArch(), driver_path.string()},
              }},
         }},
    };

    temp_dir = std::filesystem::temp_directory_path() / "adbc_driver_manager_test";
    std::filesystem::create_directories(temp_dir);
#endif
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

    driver_path.clear();
    if (std::filesystem::exists(temp_dir)) {
      std::filesystem::remove_all(temp_dir);
    }
  }

 protected:
  void SetConfigPath(const char* path) {
#ifdef _WIN32
    int size_needed = MultiByteToWideChar(CP_UTF8, 0, path, -1, nullptr, 0);
    std::wstring wpath(size_needed, 0);
    MultiByteToWideChar(CP_UTF8, 0, path, -1, &wpath[0], size_needed);
    ASSERT_TRUE(SetEnvironmentVariableW(L"ADBC_DRIVER_PATH", wpath.c_str()));
#else
    setenv("ADBC_DRIVER_PATH", path, 1);
#endif
  }

  void UnsetConfigPath() { SetConfigPath(""); }

  struct AdbcDriver driver = {};
  struct AdbcError error = {};

  std::filesystem::path driver_path;
  std::filesystem::path temp_dir;
  toml::table simple_manifest;
};

TEST_F(DriverManifest, LoadDriverEnv) {
  ASSERT_THAT(AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  std::ofstream test_manifest_file(temp_dir / "sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  SetConfigPath(temp_dir.string().c_str());

  ASSERT_THAT(AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(temp_dir / "sqlite.toml"));

  UnsetConfigPath();
}

TEST_F(DriverManifest, LoadNonAsciiPath) {
  ASSERT_THAT(AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

#ifdef _WIN32
  std::filesystem::path non_ascii_dir = temp_dir / L"majestik møøse";
#else
  std::filesystem::path non_ascii_dir = temp_dir / "majestik møøse";
#endif

  ASSERT_TRUE(std::filesystem::create_directories(non_ascii_dir));

  std::ofstream test_manifest_file(non_ascii_dir / "sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  SetConfigPath(non_ascii_dir.string().c_str());

  ASSERT_THAT(AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(non_ascii_dir / "sqlite.toml"));

  UnsetConfigPath();
}

TEST_F(DriverManifest, DisallowEnvConfig) {
  std::ofstream test_manifest_file(temp_dir / "sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  SetConfigPath(temp_dir.string().c_str());

  auto load_options = ADBC_LOAD_FLAG_DEFAULT & ~ADBC_LOAD_FLAG_SEARCH_ENV;
  ASSERT_THAT(AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0, load_options,
                                 nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  ASSERT_TRUE(std::filesystem::remove(temp_dir / "sqlite.toml"));

  UnsetConfigPath();
}

TEST_F(DriverManifest, ConfigEntrypoint) {
  auto manifest_with_bad_entrypoint = simple_manifest;
  // Override the entrypoint in the manifest
  manifest_with_bad_entrypoint.erase("Driver");
  manifest_with_bad_entrypoint.insert(
      "Driver", toml::table{
                    {"entrypoint", "BadEntrypointSymbolName"},
                    {"shared",
                     toml::table{
                         {adbc::CurrentArch(), driver_path.string()},
                     }},
                });

  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_with_bad_entrypoint;
  test_manifest_file.close();

  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, LoadAbsolutePath) {
  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, LoadAbsolutePathNoExtension) {
  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  auto noext = filepath;
  noext.replace_extension();  // Remove the .toml extension
  ASSERT_THAT(AdbcFindLoadDriver(noext.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, LoadRelativePath) {
  std::ofstream test_manifest_file("sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  ASSERT_THAT(AdbcFindLoadDriver("sqlite.toml", nullptr, ADBC_VERSION_1_1_0, 0, nullptr,
                                 &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(
      AdbcFindLoadDriver("sqlite.toml", nullptr, ADBC_VERSION_1_1_0,
                         ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS, nullptr, &driver, &error),
      IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove("sqlite.toml"));
}

TEST_F(DriverManifest, NotFound) {
  ASSERT_THAT(AdbcFindLoadDriver("nosuchdriver", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Also searched these paths for manifests:\n\tnot "
                                   "set: ADBC_DRIVER_PATH"));
}

TEST_F(DriverManifest, ManifestDriverMissing) {
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not defined in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("`Driver.shared` must be a string or table"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverMissingAdbcDatabase) {
  // Similar test as above but with AdbcDatabaseInit path and using the
  // additional search path.
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", "sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseSetLoadFlags(&database.value,
                                                    ADBC_LOAD_FLAG_DEFAULT, &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not defined in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("`Driver.shared` must be a string or table"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverInvalid) {
  // "Driver" section is not a table
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{{"shared", true}});

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not defined in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("`Driver.shared` must be a string or table"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverEmpty) {
  // "Driver" section is not a table
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{{"shared", ""}});

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Driver path is an empty string in manifest"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestWrongArch) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver",
                                 toml::table{
                                     {"shared",
                                      toml::table{
                                          {"non-existent", "path/to/bad/driver.so"},
                                          {"windows-alpha64", "path/to/bad/driver.so"},
                                      }},
                                 });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not found in manifest"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Architectures found: non-existent windows-alpha64"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverMissingArchAdbcDatabase) {
  // Similar test as above but with AdbcDatabaseInit path and using the
  // additional search path.
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver",
                                 toml::table{
                                     {"shared",
                                      toml::table{
                                          {"non-existent", "path/to/bad/driver.so"},
                                          {"windows-alpha64", "path/to/bad/driver.so"},
                                      }},
                                 });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", "sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseSetLoadFlags(&database.value,
                                                    ADBC_LOAD_FLAG_DEFAULT, &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("sqlite.toml but:"));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Architectures found: non-existent windows-alpha64"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestDriverPointsNowhere) {
  // Similar test as above but with AdbcDatabaseInit path and using the
  // additional search path.
  // Create a manifest without the "Driver" section
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  // The idea is that we can find the manifest, but not the driver it points to.
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared",
                                                toml::table{
                                                    {"linux_arm64", "adbc-goosedb"},
                                                    {"linux_amd64", "adbc-goosedb"},
                                                    {"macos_arm64", "adbc-goosedb"},
                                                    {"macos_amd64", "adbc-goosedb"},
                                                    {"windows_arm64", "adbc-goosedb"},
                                                    {"windows_amd64", "adbc-goosedb"},
                                                }},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", "sqlite", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseSetLoadFlags(&database.value,
                                                    ADBC_LOAD_FLAG_DEFAULT, &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("sqlite.toml but:"));
  // Message is platform-specific but something like "dlopen() failed:
  // adbc-goosedb: cannot open shared object file..."
  ASSERT_THAT(error.message, ::testing::HasSubstr("adbc-goosedb"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestArchPathEmpty) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared",
                                                toml::table{
                                                    {"linux_arm64", ""},
                                                    {"linux_amd64", ""},
                                                    {"macos_arm64", ""},
                                                    {"macos_amd64", ""},
                                                    {"windows_arm64", ""},
                                                    {"windows_amd64", ""},
                                                }},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Driver path is an empty string in manifest"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestArchPathInvalid) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared",
                                                toml::table{
                                                    {"linux_arm64", 42},
                                                    {"linux_amd64", 42},
                                                    {"macos_arm64", 42},
                                                    {"macos_amd64", 42},
                                                    {"windows_arm64", 42},
                                                    {"windows_amd64", 42},
                                                }},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message, ::testing::HasSubstr("Driver path not found in manifest"));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Value was not a string"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestEntrypointInvalid) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_without_driver = simple_manifest;
  manifest_without_driver.erase("Driver");
  manifest_without_driver.insert("Driver", toml::table{
                                               {"shared", "foobar"},
                                               {"entrypoint", 42},
                                           });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(error.message,
              ::testing::HasSubstr("Driver entrypoint not a string in manifest"));
  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ManifestBadVersion) {
  auto filepath = temp_dir / "sqlite.toml";
  toml::table manifest_with_bad_version = simple_manifest;
  manifest_with_bad_version.insert("manifest_version", 2);

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_with_bad_version;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

// only build and run test that puts files in the users home directory if
// it's been enabled via the build system setting this compile def
#ifdef ADBC_DRIVER_MANAGER_TEST_MANIFEST_USER_LEVEL
TEST_F(DriverManifest, LoadUserLevelManifest) {
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  auto user_config_dir = InternalAdbcUserConfigDir();
  bool created = false;
  if (!std::filesystem::exists(user_config_dir)) {
    ASSERT_TRUE(std::filesystem::create_directories(user_config_dir));
    created = true;
  }

  std::ofstream test_manifest_file(user_config_dir / "adbc-test-sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  // fail to load if flag doesn't have ADBC_LOAD_FLAG_SEARCH_USER
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0, 0,
                                 nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  // succeed with default load options
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(user_config_dir / "adbc-test-sqlite.toml"));
  if (created) {
    std::filesystem::remove_all(user_config_dir);
  }
}
#endif

// only build and run test that creates / adds a file to /etc/adbc/drivers if
// it's been enabled via the build system setting this compile def
#ifdef ADBC_DRIVER_MANAGER_TEST_MANIFEST_SYSTEM_LEVEL
TEST_F(DriverManifest, LoadSystemLevelManifest) {
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  auto system_config_dir = std::filesystem::path("/etc/adbc/drivers");
  bool created = false;
  if (!std::filesystem::exists(system_config_dir)) {
    ASSERT_TRUE(std::filesystem::create_directories(system_config_dir));
    created = true;
  }

  std::ofstream test_manifest_file(system_config_dir / "adbc-test-sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  // fail to load if flag doesn't have ADBC_LOAD_FLAG_SEARCH_SYSTEM
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0, 0,
                                 nullptr, &driver, &error),
              Not(IsOkStatus(&error)));

  // succeed with default load options
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, nullptr, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(system_config_dir / "adbc-test-sqlite.toml"));
  if (created) {
    std::filesystem::remove_all(system_config_dir);
  }
}
#endif

TEST_F(DriverManifest, AllDisabled) {
  // Test that if the user doesn't set load flags, we properly flag this
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0, 0,
                                 nullptr, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: ADBC_DRIVER_PATH (enable "
                                   "ADBC_LOAD_FLAG_SEARCH_ENV)"));

#ifdef _WIN32
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: HKEY_CURRENT_USER"));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: HKEY_LOCAL_MACHINE"));
#else
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: user config dir /"));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr("not enabled at run time: system config dir /"));
#endif  // _WIN32
  EXPECT_THAT(error.message,
              ::testing::HasSubstr(" (enable ADBC_LOAD_FLAG_SEARCH_USER)"));
  EXPECT_THAT(error.message,
              ::testing::HasSubstr(" (enable ADBC_LOAD_FLAG_SEARCH_SYSTEM)"));
}

TEST_F(DriverManifest, CondaPrefix) {
#if ADBC_CONDA_BUILD
  constexpr bool is_conda_build = true;
#else
  constexpr bool is_conda_build = false;
#endif  // ADBC_CONDA_BUILD

  std::cerr << "ADBC_CONDA_BUILD: " << (is_conda_build ? "defined" : "not defined")
            << std::endl;

  auto filepath = temp_dir / "etc" / "adbc" / "drivers" / "sqlite.toml";
  std::filesystem::create_directories(filepath.parent_path());
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

#ifdef _WIN32
  ASSERT_EQ(0, ::_wputenv_s(L"CONDA_PREFIX", temp_dir.native().c_str()));
#else
  ASSERT_EQ(0, ::setenv("CONDA_PREFIX", temp_dir.native().c_str(), 1));
#endif  // _WIN32

  AdbcStatusCode result =
      AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0, ADBC_LOAD_FLAG_DEFAULT,
                         nullptr, &driver, &error);

  if constexpr (is_conda_build) {
    ASSERT_THAT(result, IsOkStatus(&error));
  } else {
    ASSERT_THAT(result, IsStatus(ADBC_STATUS_NOT_FOUND, &error));
    ASSERT_THAT(error.message,
                ::testing::HasSubstr("not enabled at build time: Conda prefix"));
  }
}

TEST_F(DriverManifest, ImplicitUri) {
  auto filepath = temp_dir / "postgresql.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << R"([Driver]
shared = "adbc_driver_postgresql")";
  test_manifest_file.close();

  // Should attempt to load the "postgresql" driver by inferring from the URI
  std::string uri = "postgresql://a:b@localhost:9999/nonexistent";
  adbc_validation::Handle<struct AdbcDatabase> database;
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver", uri.c_str(), &error),
              IsOkStatus(&error));
  std::string search_path = temp_dir.string();
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, search_path.data(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_IO, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Failed to connect"));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, DriverFromUri) {
  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << R"([Driver]
shared = "adbc_driver_sqlite")";
  test_manifest_file.close();

  const std::string uri = "sqlite:file::memory:";
  for (const auto& driver_option : {"driver", "uri"}) {
    SCOPED_TRACE(driver_option);
    SCOPED_TRACE(uri);
    adbc_validation::Handle<struct AdbcDatabase> database;
    ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
    ASSERT_THAT(
        AdbcDatabaseSetOption(&database.value, driver_option, uri.c_str(), &error),
        IsOkStatus(&error));
    std::string search_path = temp_dir.string();
    ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                    &database.value, search_path.data(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
                IsStatus(ADBC_STATUS_OK, &error));
  }

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, DriverFromDriverAndUri) {
  // Regression test: if we set both driver and URI, then we shouldn't try to
  // extract driver from URI or vice versa.

  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << R"([Driver]
shared = "adbc_driver_sqlite")";
  test_manifest_file.close();

  const std::string uri = "foo.db";
  std::vector<std::vector<std::pair<std::string, std::string>>> options_cases = {
      {{"driver", "sqlite"}, {"uri", uri}},
      {{"uri", uri}, {"driver", "sqlite"}},
  };
  for (const auto& options : options_cases) {
    adbc_validation::Handle<struct AdbcDatabase> database;
    ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
    for (const auto& option : options) {
      ASSERT_THAT(AdbcDatabaseSetOption(&database.value, option.first.c_str(),
                                        option.second.c_str(), &error),
                  IsOkStatus(&error));
    }

    std::string search_path = temp_dir.string();
    ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                    &database.value, search_path.data(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  }

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, ControlCodes) {
  const std::string uri = "\026sqlite:file::memory:";
  for (const auto& driver_option : {"driver", "uri"}) {
    SCOPED_TRACE(driver_option);
    SCOPED_TRACE(uri);
    adbc_validation::Handle<struct AdbcDatabase> database;
    ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
    ASSERT_THAT(
        AdbcDatabaseSetOption(&database.value, driver_option, uri.c_str(), &error),
        IsOkStatus(&error));
    std::string search_path = temp_dir.string();
    ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                    &database.value, search_path.data(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
    ASSERT_THAT(
        error.message,
        ::testing::HasSubstr(
            "Note: driver name may have non-printable characters: `\\x16sqlite`"));
  }
}

class ConnectionProfiles : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

    temp_dir =
        std::filesystem::temp_directory_path() / "adbc_driver_manager_profile_test";
    std::filesystem::create_directories(temp_dir);

    simple_profile = toml::table{
        {"version", 1},
        {"driver", "adbc_driver_sqlite"},
        {"options",
         toml::table{
             {"uri", "file::memory:"},
         }},
    };
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

    driver_path.clear();
    if (std::filesystem::exists(temp_dir)) {
      std::filesystem::remove_all(temp_dir);
    }
  }

 protected:
  void SetConfigPath(const char* path) {
#ifdef _WIN32
    int size_needed = MultiByteToWideChar(CP_UTF8, 0, path, -1, nullptr, 0);
    std::wstring wpath(size_needed, 0);
    MultiByteToWideChar(CP_UTF8, 0, path, -1, &wpath[0], size_needed);
    ASSERT_TRUE(SetEnvironmentVariableW(L"ADBC_PROFILE_PATH", wpath.c_str()));
#else
    setenv("ADBC_PROFILE_PATH", path, 1);
#endif
  }

  void UnsetConfigPath() { SetConfigPath(""); }

  struct AdbcDriver driver = {};
  struct AdbcError error = {};

  std::filesystem::path temp_dir;
  std::filesystem::path driver_path;
  toml::table simple_profile;
};

TEST_F(ConnectionProfiles, SetProfileOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", filepath.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // inherit additional_search_path_list
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDriverManagerDatabaseSetAdditionalSearchPathList(
                  &database.value, temp_dir.string().c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // find profile by name using ADBC_PROFILE_PATH
  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, HierarchicalProfile) {
  auto filepath = temp_dir / "dev" / "profile.toml";
  std::filesystem::create_directories(filepath.parent_path());
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "dev/profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, UriProfileOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri",
                                    ("profile://" + filepath.string()).c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // find profile by name using ADBC_PROFILE_PATH
  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "uri", "profile://profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, DriverProfileOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "driver",
                                    ("profile://" + filepath.string()).c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));

  // find profile by name using ADBC_PROFILE_PATH
  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDatabaseSetOption(&database.value, "driver", "profile://profile", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database.value, &error), IsOkStatus(&error));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, ExtraStringOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  profile["options"].as_table()->insert("foo", "bar");
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo='bar'"));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, ExtraIntOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  profile["options"].as_table()->insert("foo", int64_t(42));
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo=42"));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, ExtraDoubleOption) {
  auto filepath = temp_dir / "profile.toml";
  toml::table profile = simple_profile;
  profile["options"].as_table()->insert("foo", 42.0);
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << profile;
  test_manifest_file.close();

  adbc_validation::Handle<struct AdbcDatabase> database;

  // find profile by name using ADBC_PROFILE_PATH
  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown database option foo=42"));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, ProfileNotFound) {
  adbc_validation::Handle<struct AdbcDatabase> database;

  // absolute path to the profile
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile",
                                    (temp_dir / "profile.toml").c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("Profile file does not exist: " +
                                                  (temp_dir / "profile.toml").string()));

  // find profile by name using ADBC_PROFILE_PATH
  SetConfigPath(temp_dir.c_str());
  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_THAT(error.message,
              ::testing::HasSubstr(std::string("Profile not found: profile\n") +
                                   "Also searched these paths for profiles:\n\t" +
                                   "ADBC_PROFILE_PATH: " + temp_dir.string() + "\n\t"));
  UnsetConfigPath();
}

TEST_F(ConnectionProfiles, CustomProfileProvider) {
  adbc_validation::Handle<struct AdbcDatabase> database;

  AdbcConnectionProfileProvider provider =
      [](const char* profile_name, const char* additional_path_list,
         struct AdbcConnectionProfile* out, struct AdbcError* error) -> AdbcStatusCode {
    EXPECT_EQ(std::string(profile_name), "profile");

    static const std::string expected = "custom profile provider error";
    error->message = new char[expected.size() + 1];
    std::copy(expected.begin(), expected.end(), error->message);
    error->message[expected.size()] = '\0';
    error->release = [](struct AdbcError* error) {
      delete[] error->message;
      error->message = nullptr;
      error->release = nullptr;
    };
    return ADBC_STATUS_INVALID_ARGUMENT;
  };

  ASSERT_THAT(AdbcDatabaseNew(&database.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOption(&database.value, "profile", "profile", &error),
              IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDriverManagerDatabaseSetProfileProvider(&database.value, provider, &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database.value, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("custom profile provider error"));
}

}  // namespace adbc
