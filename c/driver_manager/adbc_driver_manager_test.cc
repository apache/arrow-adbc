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

#include <memory>
#include <string>
#include <vector>

#include "adbc.h"
#include "adbc_driver_manager.h"
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

// Tests of the SQLite example driver, except using the driver manager

namespace adbc {

using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;

class DriverManager : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

    ASSERT_THAT(AdbcLoadDriver("adbc_driver_sqlite", nullptr, ADBC_VERSION_1_0_0, &driver,
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
  ASSERT_THAT(error.message, ::testing::HasSubstr("Unknown connection option foo=bar"));

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
  ASSERT_THAT(AdbcDatabaseSetOption(&postgres_db.value, "driver", "adbc_driver_postgres",
                                    &error.value),
              IsOkStatus(&error.value));

  ASSERT_THAT(AdbcDatabaseInit(&sqlite_db.value, &error.value), IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseInit(&postgres_db.value, &error.value),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error.value));
  ASSERT_THAT(error->message,
              ::testing::HasSubstr(
                  "[libpq] Must set database option 'uri' before creating a connection"));
  error->release(&error.value);

  ASSERT_THAT(AdbcDatabaseSetOption(&postgres_db.value, "uri",
                                    "postgres://localhost:5432", &error.value),
              IsOkStatus(&error.value));
  ASSERT_THAT(AdbcDatabaseSetOption(&sqlite_db.value, "unknown", "foo", &error.value),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error.value));
  ASSERT_THAT(error->message,
              ::testing::HasSubstr("[SQLite] Unknown database option unknown=foo"));
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

  bool supports_concurrent_statements() const override { return true; }
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

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SqliteStatementTest)

}  // namespace adbc
