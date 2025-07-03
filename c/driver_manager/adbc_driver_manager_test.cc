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

#include <stdlib.h>
#include <algorithm>
#include <filesystem>  // NOLINT [build/c++17]
#include <memory>
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

    auto temp_path = std::filesystem::temp_directory_path();
    temp_path /= "adbc_driver_manager_test";

    ASSERT_TRUE(std::filesystem::create_directories(temp_path));
    temp_dir = temp_path;
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
    ASSERT_TRUE(SetEnvironmentVariable("ADBC_CONFIG_PATH", path));
#else
    setenv("ADBC_CONFIG_PATH", path, 1);
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
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              Not(IsOkStatus(&error)));

  std::ofstream test_manifest_file(temp_dir / "sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  SetConfigPath(temp_dir.string().c_str());

  ASSERT_THAT(AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(temp_dir / "sqlite.toml"));

  UnsetConfigPath();
}

TEST_F(DriverManifest, LoadNonAsciiPath) {
  ASSERT_THAT(AdbcFindLoadDriver("sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
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
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
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
                                 &driver, &error),
              Not(IsOkStatus(&error)));

  ASSERT_TRUE(std::filesystem::remove(temp_dir / "sqlite.toml"));

  UnsetConfigPath();
}

TEST_F(DriverManifest, LoadAbsolutePath) {
  auto filepath = temp_dir / "sqlite.toml";
  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
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
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

TEST_F(DriverManifest, LoadRelativePath) {
  std::ofstream test_manifest_file("sqlite.toml");
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << simple_manifest;
  test_manifest_file.close();

  ASSERT_THAT(
      AdbcFindLoadDriver("sqlite.toml", nullptr, ADBC_VERSION_1_1_0, 0, &driver, &error),
      IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));

  ASSERT_THAT(AdbcFindLoadDriver("sqlite.toml", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_ALLOW_RELATIVE_PATHS, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove("sqlite.toml"));
}

TEST_F(DriverManifest, ManifestMissingDriver) {
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
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));

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
                                      }},
                                 });

  std::ofstream test_manifest_file(filepath);
  ASSERT_TRUE(test_manifest_file.is_open());
  test_manifest_file << manifest_without_driver;
  test_manifest_file.close();

  // Attempt to load the driver
  ASSERT_THAT(AdbcFindLoadDriver(filepath.string().data(), nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));

  ASSERT_TRUE(std::filesystem::remove(filepath));
}

// only build and run test that puts files in the users home directory if
// it's been enabled via the build system setting this compile def
#ifdef ADBC_DRIVER_MANAGER_TEST_MANIFEST_USER_LEVEL
TEST_F(DriverManifest, LoadUserLevelManifest) {
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
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
                                 &driver, &error),
              Not(IsOkStatus(&error)));

  // succeed with default load options
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(user_config_dir / "adbc-test-sqlite.toml"));
  if (created) {
    std::filesystem::remove_all(user_config_dir);
  }
}
#endif

// only build and run test that creates / adds a file to /etc/adbc if
// it's been enabled via the build system setting this compile def
#ifdef ADBC_DRIVER_MANAGER_TEST_MANIFEST_SYSTEM_LEVEL
TEST_F(DriverManifest, LoadSystemLevelManifest) {
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              Not(IsOkStatus(&error)));

  auto system_config_dir = std::filesystem::path("/etc/adbc");
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
                                 &driver, &error),
              Not(IsOkStatus(&error)));

  // succeed with default load options
  ASSERT_THAT(AdbcFindLoadDriver("adbc-test-sqlite", nullptr, ADBC_VERSION_1_1_0,
                                 ADBC_LOAD_FLAG_DEFAULT, &driver, &error),
              IsOkStatus(&error));

  ASSERT_TRUE(std::filesystem::remove(system_config_dir / "adbc-test-sqlite.toml"));
  if (created) {
    std::filesystem::remove_all(system_config_dir);
  }
}
#endif

}  // namespace adbc
