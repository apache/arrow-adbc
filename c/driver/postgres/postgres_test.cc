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
#include <cstring>

#include <adbc.h>
#include <gtest/gtest.h>

#include "validation/adbc_validation.h"

class PostgresQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    const char* uri = std::getenv("ADBC_POSTGRES_TEST_URI");
    if (!uri) {
      ADD_FAILURE() << "Must provide env var ADBC_POSTGRES_TEST_URI";
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return AdbcDatabaseSetOption(database, "uri", uri, error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    struct AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    AdbcStatusCode status = AdbcStatementNew(connection, &statement, error);
    if (status != ADBC_STATUS_OK) return status;

    std::string query = "DROP TABLE IF EXISTS " + name;
    status = AdbcStatementSetSqlQuery(&statement, query.c_str(), error);
    if (status != ADBC_STATUS_OK) {
      std::ignore = AdbcStatementRelease(&statement, error);
      return status;
    }
    status = AdbcStatementExecuteQuery(&statement, nullptr, nullptr, error);
    std::ignore = AdbcStatementRelease(&statement, error);
    return status;
  }

  std::string BindParameter(int index) const override {
    return "$" + std::to_string(index + 1);
  }
};

class PostgresDatabaseTest : public ::testing::Test,
                             public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  PostgresQuirks quirks_;
};
ADBCV_TEST_DATABASE(PostgresDatabaseTest)

class PostgresConnectionTest : public ::testing::Test,
                               public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestMetadataGetInfo() { GTEST_SKIP() << "Not yet implemented"; }
  void TestMetadataGetTableSchema() { GTEST_SKIP() << "Not yet implemented"; }
  void TestMetadataGetTableTypes() { GTEST_SKIP() << "Not yet implemented"; }

  void TestMetadataGetObjectsCatalogs() { GTEST_SKIP() << "Not yet implemented"; }
  void TestMetadataGetObjectsDbSchemas() { GTEST_SKIP() << "Not yet implemented"; }
  void TestMetadataGetObjectsTables() { GTEST_SKIP() << "Not yet implemented"; }
  void TestMetadataGetObjectsTablesTypes() { GTEST_SKIP() << "Not yet implemented"; }
  void TestMetadataGetObjectsColumns() { GTEST_SKIP() << "Not yet implemented"; }

 protected:
  PostgresQuirks quirks_;
};
ADBCV_TEST_CONNECTION(PostgresConnectionTest)

class PostgresStatementTest : public ::testing::Test,
                              public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestSqlPrepareErrorParamCountMismatch() { GTEST_SKIP() << "Not yet implemented"; }
  void TestSqlPrepareGetParameterSchema() { GTEST_SKIP() << "Not yet implemented"; }
  void TestSqlPrepareSelectParams() { GTEST_SKIP() << "Not yet implemented"; }

  void TestConcurrentStatements() {
    // TODO: refactor driver so that we read all the data as soon as
    // we ExecuteQuery() since that's how libpq already works - then
    // we can actually support concurrent statements (because there is
    // no concurrency)
    GTEST_SKIP() << "Not yet implemented";
  }

 protected:
  PostgresQuirks quirks_;
};
ADBCV_TEST_STATEMENT(PostgresStatementTest)
