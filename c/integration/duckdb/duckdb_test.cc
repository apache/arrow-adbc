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

#include <cstdlib>
#include <stdexcept>
#include <string>
#include "gmock/gmock.h"

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/adbc_driver_manager.h>
#define ADBC_EXPORTING  // duckdb changed the include guard...
#include <duckdb/common/adbc/adbc-init.hpp>

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

// Convert between our definitions and DuckDB's
AdbcStatusCode DuckDbDriverInitFunc(int version, void* driver, struct AdbcError* error) {
  return duckdb_adbc_init(static_cast<size_t>(version), driver, error);
}

class DuckDbQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    if (auto status =
            AdbcDriverManagerDatabaseSetInitFunc(database, DuckDbDriverInitFunc, error);
        status != ADBC_STATUS_OK) {
      return status;
    }

    return ADBC_STATUS_OK;
  }

  std::string BindParameter(int index) const override { return "?"; }

  bool supports_bulk_ingest(const char* /*mode*/) const override { return false; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_dynamic_parameter_binding() const override { return false; }
  bool supports_get_sql_info() const override { return false; }
  bool supports_get_objects() const override { return false; }
  bool supports_rows_affected() const override { return false; }
  bool supports_transactions() const override { return false; }
};

class DuckDbDatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  DuckDbQuirks quirks_;
};
ADBCV_TEST_DATABASE(DuckDbDatabaseTest)

class DuckDbConnectionTest : public ::testing::Test,
                             public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestAutocommitDefault() { GTEST_SKIP(); }
  void TestMetadataGetTableSchema() { GTEST_SKIP(); }
  void TestMetadataGetTableSchemaNotFound() { GTEST_SKIP(); }
  void TestMetadataGetTableTypes() { GTEST_SKIP(); }

 protected:
  DuckDbQuirks quirks_;
};
ADBCV_TEST_CONNECTION(DuckDbConnectionTest)

class DuckDbStatementTest : public ::testing::Test,
                            public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  // DuckDB doesn't guard against this
  void TestNewInit() { GTEST_SKIP(); }
  // Accepts Prepare() without any query
  void TestSqlPrepareErrorNoQuery() { GTEST_SKIP(); }

  void TestSqlIngestTableEscaping() { GTEST_SKIP() << "Table escaping not implemented"; }
  void TestSqlIngestColumnEscaping() {
    GTEST_SKIP() << "Column escaping not implemented";
  }

  void TestSqlQueryErrors() { GTEST_SKIP() << "DuckDB does not set AdbcError.release"; }
  void TestSqlQueryRowsAffectedDelete() {
    GTEST_SKIP() << "Cannot query rows affected in delete (not implemented)";
  }
  void TestSqlQueryRowsAffectedDeleteStream() {
    GTEST_SKIP() << "Cannot query rows affected in delete stream (not implemented)";
  }

  void TestSqlQueryTrailingSemicolons() {
    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error),
                adbc_validation::IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "INSTALL icu", &error),
                adbc_validation::IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                adbc_validation::IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "LOAD icu", &error),
                adbc_validation::IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                adbc_validation::IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementRelease(&statement, &error),
                adbc_validation::IsOkStatus(&error));

    adbc_validation::StatementTest::TestSqlQueryTrailingSemicolons();
  }

  void TestErrorCompatibility() {
    GTEST_SKIP() << "DuckDB does not set AdbcError.release";
  }

  void TestResultIndependence() {
    // DuckDB detects this by throwing
    ASSERT_THAT([this]() { adbc_validation::StatementTest::TestResultIndependence(); },
                ::testing::Throws<std::runtime_error>());
  }

 protected:
  DuckDbQuirks quirks_;
};
ADBCV_TEST_STATEMENT(DuckDbStatementTest)
