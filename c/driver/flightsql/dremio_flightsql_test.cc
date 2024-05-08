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

class DremioFlightSqlQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    const char* uri = std::getenv("ADBC_DREMIO_FLIGHTSQL_URI");
    const char* user = std::getenv("ADBC_DREMIO_FLIGHTSQL_USER");
    const char* pass = std::getenv("ADBC_DREMIO_FLIGHTSQL_PASS");
    EXPECT_THAT(AdbcDatabaseSetOption(database, "uri", uri, error), IsOkStatus(error));
    EXPECT_THAT(AdbcDatabaseSetOption(database, "username", user, error),
                IsOkStatus(error));
    EXPECT_THAT(AdbcDatabaseSetOption(database, "password", pass, error),
                IsOkStatus(error));
    return ADBC_STATUS_OK;
  }

  std::string BindParameter(int index) const override { return "?"; }
  bool supports_bulk_ingest(const char* /*mode*/) const override { return false; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return false; }
  bool supports_get_sql_info() const override { return false; }
  bool supports_get_objects() const override { return true; }
  bool supports_partitioned_data() const override { return true; }
  bool supports_dynamic_parameter_binding() const override { return false; }
};

class DremioFlightSqlTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  DremioFlightSqlQuirks quirks_;
};
ADBCV_TEST_DATABASE(DremioFlightSqlTest)

class DremioFlightSqlConnectionTest : public ::testing::Test,
                                      public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestMetadataGetObjectsCatalogs() { GTEST_SKIP() << "Dremio reports no catalogs"; }
  void TestMetadataGetObjectsDbSchemas() { GTEST_SKIP() << "Ingestion not supported"; }
  void TestMetadataGetObjectsTables() { GTEST_SKIP() << "Ingestion not supported"; }
  void TestMetadataGetObjectsTablesTypes() { GTEST_SKIP() << "Ingestion not supported"; }
  void TestMetadataGetObjectsColumns() { GTEST_SKIP() << "Ingestion not supported"; }

 protected:
  DremioFlightSqlQuirks quirks_;
};
ADBCV_TEST_CONNECTION(DremioFlightSqlConnectionTest)

class DremioFlightSqlStatementTest : public ::testing::Test,
                                     public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestResultInvalidation() { GTEST_SKIP() << "Dremio generates a CANCELLED"; }
  void TestSqlIngestTableEscaping() { GTEST_SKIP() << "Table escaping not implemented"; }
  void TestSqlIngestColumnEscaping() {
    GTEST_SKIP() << "Column escaping not implemented";
  }
  void TestSqlQueryEmpty() { GTEST_SKIP() << "Dremio doesn't support 'acceptPut'"; }
  void TestSqlQueryRowsAffectedDelete() {
    GTEST_SKIP() << "Cannot query rows affected in delete (not implemented)";
  }
  void TestSqlQueryRowsAffectedDeleteStream() {
    GTEST_SKIP() << "Cannot query rows affected in delete stream (not implemented)";
  }

 protected:
  DremioFlightSqlQuirks quirks_;
};
ADBCV_TEST_STATEMENT(DremioFlightSqlStatementTest)
