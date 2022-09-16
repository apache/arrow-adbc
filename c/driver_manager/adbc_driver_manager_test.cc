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

#include <memory>
#include <string>
#include <vector>

#include "adbc.h"
#include "adbc_driver_manager.h"
#include "driver/test_util.h"
#include "validation/adbc_validation.h"

// Tests of the SQLite example driver, except using the driver manager

namespace adbc {

class DriverManager : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));

    ADBC_ASSERT_OK_WITH_ERROR(error, AdbcLoadDriver("adbc_driver_sqlite", NULL,
                                                    ADBC_VERSION_1_0_0, &driver, &error));
  }

  void TearDown() override {
    if (error.release) {
      error.release(&error);
    }

    if (driver.release) {
      ADBC_ASSERT_OK_WITH_ERROR(error, driver.release(&driver, &error));
      ASSERT_EQ(driver.private_data, nullptr);
      ASSERT_EQ(driver.private_manager, nullptr);
    }
  }

 protected:
  AdbcDriver driver;
  AdbcError error = {};
};

TEST_F(DriverManager, DatabaseCustomInitFunc) {
  AdbcDatabase database;
  std::memset(&database, 0, sizeof(database));

  // Explicitly set entrypoint
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseNew(&database, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcDatabaseSetOption(&database, "entrypoint", "AdbcDriverInit", &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseInit(&database, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseRelease(&database, &error));

  // Set invalid entrypoint
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseNew(&database, &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error, AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", &error));
  ADBC_ASSERT_OK_WITH_ERROR(
      error,
      AdbcDatabaseSetOption(&database, "entrypoint", "ThisSymbolDoesNotExist", &error));
  ASSERT_EQ(ADBC_STATUS_INTERNAL, AdbcDatabaseInit(&database, &error));
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcDatabaseRelease(&database, &error));
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
        database, "filename", "file:Sqlite_Transactions?mode=memory&cache=shared", error);
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
