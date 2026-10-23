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

#include <gtest/gtest.h>
#include <cstring>

#include <arrow-adbc/adbc.h>
#include "driver/framework/base_driver.h"
#include "driver/framework/client.h"
#include "driver/framework/connection.h"
#include "driver/framework/database.h"
#include "driver/framework/statement.h"

// Self-contained version of the Handle
static inline void clean_up(AdbcDriver* ptr) { ptr->release(ptr, nullptr); }

static inline void clean_up(AdbcDatabase* ptr) {
  ptr->private_driver->DatabaseRelease(ptr, nullptr);
}

static inline void clean_up(AdbcConnection* ptr) {
  ptr->private_driver->ConnectionRelease(ptr, nullptr);
}

static inline void clean_up(AdbcStatement* ptr) {
  ptr->private_driver->StatementRelease(ptr, nullptr);
}

template <typename T>
class Handle {
 public:
  explicit Handle(T* value) : value_(value) {}

  ~Handle() { clean_up(value_); }

 private:
  T* value_;
};

namespace {

class BaseVoidDatabase : public adbc::driver::BaseDatabase<BaseVoidDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";
};

class BaseVoidConnection : public adbc::driver::BaseConnection<BaseVoidConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";
};

class BaseVoidStatement : public adbc::driver::BaseStatement<BaseVoidStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";
};

using BaseVoidDriver =
    adbc::driver::Driver<BaseVoidDatabase, BaseVoidConnection, BaseVoidStatement>;
}  // namespace

AdbcStatusCode BaseVoidDriverInitFunc(int version, void* raw_driver, AdbcError* error) {
  return BaseVoidDriver::Init(version, raw_driver, error);
}

TEST(TestDriverBase, TestBaseVoidDriverMethods) {
  // Checks that wires are plugged in for a framework-based driver based only on what is
  // available in base_driver.h

  struct AdbcDriver driver;
  memset(&driver, 0, sizeof(driver));
  ASSERT_EQ(BaseVoidDriverInitFunc(ADBC_VERSION_1_1_0, &driver, nullptr), ADBC_STATUS_OK);
  Handle<AdbcDriver> driver_handle(&driver);

  // Database methods are only option related
  struct AdbcDatabase database;
  memset(&database, 0, sizeof(database));
  ASSERT_EQ(driver.DatabaseNew(&database, nullptr), ADBC_STATUS_OK);
  database.private_driver = &driver;
  Handle<AdbcDatabase> database_handle(&database);
  ASSERT_EQ(driver.DatabaseInit(&database, nullptr), ADBC_STATUS_OK);

  // Test connection methods
  struct AdbcConnection connection;
  memset(&connection, 0, sizeof(connection));
  ASSERT_EQ(driver.ConnectionNew(&connection, nullptr), ADBC_STATUS_OK);
  connection.private_driver = &driver;
  Handle<AdbcConnection> connection_handle(&connection);
  ASSERT_EQ(driver.ConnectionInit(&connection, &database, nullptr), ADBC_STATUS_OK);

  EXPECT_EQ(driver.ConnectionCommit(&connection, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetInfo(&connection, nullptr, 0, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetObjects(&connection, 0, nullptr, nullptr, 0, nullptr,
                                        nullptr, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetTableSchema(&connection, nullptr, nullptr, nullptr,
                                            nullptr, nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(driver.ConnectionGetTableTypes(&connection, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionReadPartition(&connection, nullptr, 0, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionRollback(&connection, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionCancel(&connection, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetStatistics(&connection, nullptr, nullptr, nullptr, 0,
                                           nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetStatisticNames(&connection, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);

  // Test statement methods
  struct AdbcStatement statement;
  memset(&statement, 0, sizeof(statement));
  ASSERT_EQ(driver.StatementNew(&connection, &statement, nullptr), ADBC_STATUS_OK);
  statement.private_driver = &driver;
  Handle<AdbcStatement> statement_handle(&statement);

  EXPECT_EQ(driver.StatementExecuteQuery(&statement, nullptr, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementExecuteSchema(&statement, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementPrepare(&statement, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementSetSqlQuery(&statement, "", nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementSetSubstraitPlan(&statement, nullptr, 0, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementBind(&statement, nullptr, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementBindStream(&statement, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementCancel(&statement, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);
}

namespace {

class VoidDatabase : public adbc::driver::Database<VoidDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";
};

class VoidConnection : public adbc::driver::Connection<VoidConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";
};

class VoidStatement : public adbc::driver::Statement<VoidStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";
};

using VoidDriver = adbc::driver::Driver<VoidDatabase, VoidConnection, VoidStatement>;
}  // namespace

AdbcStatusCode VoidDriverInitFunc(int version, void* raw_driver, AdbcError* error) {
  return VoidDriver::Init(version, raw_driver, error);
}

TEST(TestDriverBase, TestVoidDriverMethods) {
  // Checks that wires are plugged in for a framework-based driver based on
  // the more-batteries-included Database, Connection, and Statement

  struct AdbcDriver driver;
  memset(&driver, 0, sizeof(driver));
  ASSERT_EQ(VoidDriverInitFunc(ADBC_VERSION_1_1_0, &driver, nullptr), ADBC_STATUS_OK);
  Handle<AdbcDriver> driver_handle(&driver);

  // Database methods are only option related
  struct AdbcDatabase database;
  memset(&database, 0, sizeof(database));
  ASSERT_EQ(driver.DatabaseNew(&database, nullptr), ADBC_STATUS_OK);
  database.private_driver = &driver;
  Handle<AdbcDatabase> database_handle(&database);
  ASSERT_EQ(driver.DatabaseInit(&database, nullptr), ADBC_STATUS_OK);

  // Test connection methods
  struct AdbcConnection connection;
  memset(&connection, 0, sizeof(connection));
  ASSERT_EQ(driver.ConnectionNew(&connection, nullptr), ADBC_STATUS_OK);
  connection.private_driver = &driver;
  Handle<AdbcConnection> connection_handle(&connection);
  ASSERT_EQ(driver.ConnectionInit(&connection, &database, nullptr), ADBC_STATUS_OK);

  EXPECT_EQ(driver.ConnectionCommit(&connection, nullptr), ADBC_STATUS_INVALID_STATE);
  EXPECT_EQ(driver.ConnectionGetInfo(&connection, nullptr, 0, nullptr, nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(driver.ConnectionGetObjects(&connection, 0, nullptr, nullptr, 0, nullptr,
                                        nullptr, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetTableSchema(&connection, nullptr, nullptr, nullptr,
                                            nullptr, nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(driver.ConnectionGetTableTypes(&connection, nullptr, nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(driver.ConnectionReadPartition(&connection, nullptr, 0, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionRollback(&connection, nullptr), ADBC_STATUS_INVALID_STATE);
  EXPECT_EQ(driver.ConnectionCancel(&connection, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetStatistics(&connection, nullptr, nullptr, nullptr, 0,
                                           nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.ConnectionGetStatisticNames(&connection, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);

  // Test statement methods
  struct AdbcStatement statement;
  memset(&statement, 0, sizeof(statement));
  ASSERT_EQ(driver.StatementNew(&connection, &statement, nullptr), ADBC_STATUS_OK);
  statement.private_driver = &driver;
  Handle<AdbcStatement> statement_handle(&statement);

  EXPECT_EQ(driver.StatementExecuteQuery(&statement, nullptr, nullptr, nullptr),
            ADBC_STATUS_INVALID_STATE);
  EXPECT_EQ(driver.StatementExecuteSchema(&statement, nullptr, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementPrepare(&statement, nullptr), ADBC_STATUS_INVALID_STATE);
  EXPECT_EQ(driver.StatementSetSqlQuery(&statement, "", nullptr), ADBC_STATUS_OK);
  EXPECT_EQ(driver.StatementSetSubstraitPlan(&statement, nullptr, 0, nullptr),
            ADBC_STATUS_NOT_IMPLEMENTED);
  EXPECT_EQ(driver.StatementBind(&statement, nullptr, nullptr, nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(driver.StatementBindStream(&statement, nullptr, nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);
  EXPECT_EQ(driver.StatementCancel(&statement, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);
}

class TestContext : public adbc::client::Context {
  void Log(LogLevel level, std::string_view message) override {
    GTEST_FAIL() << "Unexpected TestContext log message: " << message;
  }
};

TEST(TestDriverBase, TestVoidDriverMethodsClient) {
  using adbc::client::Connection;
  using adbc::client::Database;
  using adbc::client::Driver;
  using adbc::client::Statement;

  Driver driver(std::make_shared<TestContext>());
  ASSERT_TRUE(driver.Load(VoidDriverInitFunc).ok());

  auto maybe_database = driver.NewDatabase();
  ASSERT_TRUE(maybe_database.has_value());
  Database database = std::move(maybe_database.value());

  // TODO: Test database methods

  auto maybe_connection = database.NewConnection();
  ASSERT_TRUE(maybe_connection.has_value()) << maybe_connection.status().message();
  Connection connection = std::move(maybe_connection.value());

  // TODO: Test connection methods

  // EXPECT_EQ(driver.ConnectionCommit(&connection, nullptr), ADBC_STATUS_INVALID_STATE);
  // EXPECT_EQ(driver.ConnectionGetInfo(&connection, nullptr, 0, nullptr, nullptr),
  //           ADBC_STATUS_INVALID_ARGUMENT);
  // EXPECT_EQ(driver.ConnectionGetObjects(&connection, 0, nullptr, nullptr, 0, nullptr,
  //                                       nullptr, nullptr, nullptr),
  //           ADBC_STATUS_NOT_IMPLEMENTED);
  // EXPECT_EQ(driver.ConnectionGetTableSchema(&connection, nullptr, nullptr, nullptr,
  //                                           nullptr, nullptr),
  //           ADBC_STATUS_INVALID_ARGUMENT);
  // EXPECT_EQ(driver.ConnectionGetTableTypes(&connection, nullptr, nullptr),
  //           ADBC_STATUS_INVALID_ARGUMENT);
  // EXPECT_EQ(driver.ConnectionReadPartition(&connection, nullptr, 0, nullptr, nullptr),
  //           ADBC_STATUS_NOT_IMPLEMENTED);
  // EXPECT_EQ(driver.ConnectionRollback(&connection, nullptr),
  // ADBC_STATUS_INVALID_STATE); EXPECT_EQ(driver.ConnectionCancel(&connection, nullptr),
  // ADBC_STATUS_NOT_IMPLEMENTED); EXPECT_EQ(driver.ConnectionGetStatistics(&connection,
  // nullptr, nullptr, nullptr, 0,
  //                                          nullptr, nullptr),
  //           ADBC_STATUS_NOT_IMPLEMENTED);
  // EXPECT_EQ(driver.ConnectionGetStatisticNames(&connection, nullptr, nullptr),
  //           ADBC_STATUS_NOT_IMPLEMENTED);

  auto maybe_statement = connection.NewStatement();
  ASSERT_TRUE(maybe_statement.has_value());
  Statement statement = std::move(maybe_statement.value());

  // TODO: Test statement methods
  // EXPECT_EQ(driver.StatementExecuteQuery(&statement, nullptr, nullptr, nullptr),
  //           ADBC_STATUS_INVALID_STATE);
  // EXPECT_EQ(driver.StatementExecuteSchema(&statement, nullptr, nullptr),
  //           ADBC_STATUS_NOT_IMPLEMENTED);
  // EXPECT_EQ(driver.StatementPrepare(&statement, nullptr), ADBC_STATUS_INVALID_STATE);
  // EXPECT_EQ(driver.StatementSetSqlQuery(&statement, "", nullptr), ADBC_STATUS_OK);
  // EXPECT_EQ(driver.StatementSetSubstraitPlan(&statement, nullptr, 0, nullptr),
  //           ADBC_STATUS_NOT_IMPLEMENTED);
  // EXPECT_EQ(driver.StatementBind(&statement, nullptr, nullptr, nullptr),
  //           ADBC_STATUS_INVALID_ARGUMENT);
  // EXPECT_EQ(driver.StatementBindStream(&statement, nullptr, nullptr),
  //           ADBC_STATUS_INVALID_ARGUMENT);
  // EXPECT_EQ(driver.StatementCancel(&statement, nullptr), ADBC_STATUS_NOT_IMPLEMENTED);

  ASSERT_EQ(statement.SetSqlQuery("").code(), ADBC_STATUS_OK);

  ASSERT_EQ(statement.Release().code(), ADBC_STATUS_OK);
  ASSERT_EQ(connection.Release().code(), ADBC_STATUS_OK);
  ASSERT_EQ(database.Release().code(), ADBC_STATUS_OK);
  ASSERT_EQ(driver.Unload().code(), ADBC_STATUS_OK);
}
