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
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkStatus;

class SqliteFlightSqlQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    const char* uri = std::getenv("ADBC_SQLITE_FLIGHTSQL_URI");
    EXPECT_THAT(AdbcDatabaseSetOption(database, "uri", uri, error), IsOkStatus(error));
    return ADBC_STATUS_OK;
  }

  std::string BindParameter(int index) const override { return "?"; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return false; }
  bool supports_get_sql_info() const override { return true; }
  bool supports_get_objects() const override { return false; }
  bool supports_bulk_ingest() const override { return false; }
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

 protected:
  SqliteFlightSqlQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SqliteFlightSqlStatementTest)
