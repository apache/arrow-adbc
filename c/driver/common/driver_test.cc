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

#include <adbc.h>
#include "driver_base.h"

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

class VoidDatabase : public adbc::common::DatabaseObjectBase {};

class VoidConnection : public adbc::common::ConnectionObjectBase {};

class VoidStatement : public adbc::common::StatementObjectBase {};

using VoidDriver = adbc::common::Driver<VoidDatabase, VoidConnection, VoidStatement>;

AdbcStatusCode VoidDriverInitFunc(int version, void* raw_driver, AdbcError* error) {
  return VoidDriver::Init(version, raw_driver, error);
}

TEST(TestDriverBase, TestVoidDriverMethods) {
  struct AdbcDriver driver;
  memset(&driver, 0, sizeof(driver));
  ASSERT_EQ(VoidDriverInitFunc(ADBC_VERSION_1_1_0, &driver, nullptr), ADBC_STATUS_OK);
  Handle<AdbcDriver> driver_handle(&driver);

  struct AdbcDatabase database;
  memset(&database, 0, sizeof(database));
  ASSERT_EQ(driver.DatabaseNew(&database, nullptr), ADBC_STATUS_OK);
  database.private_driver = &driver;
  Handle<AdbcDatabase> database_handle(&database);
  ASSERT_EQ(driver.DatabaseInit(&database, nullptr), ADBC_STATUS_OK);

  struct AdbcConnection connection;
  memset(&connection, 0, sizeof(connection));
  ASSERT_EQ(driver.ConnectionNew(&connection, nullptr), ADBC_STATUS_OK);
  connection.private_driver = &driver;
  Handle<AdbcConnection> connection_handle(&connection);
  ASSERT_EQ(driver.ConnectionInit(&connection, &database, nullptr), ADBC_STATUS_OK);

  struct AdbcStatement statement;
  memset(&statement, 0, sizeof(statement));
  ASSERT_EQ(driver.StatementNew(&connection, &statement, nullptr), ADBC_STATUS_OK);
  statement.private_driver = &driver;
  Handle<AdbcStatement> statement_handle(&statement);
}

TEST(TestDriverBase, TestVoidDriverOptions) {
  struct AdbcDriver driver;
  memset(&driver, 0, sizeof(driver));
  ASSERT_EQ(VoidDriverInitFunc(ADBC_VERSION_1_1_0, &driver, nullptr), ADBC_STATUS_OK);
  Handle<AdbcDriver> driver_handle(&driver);

  struct AdbcDatabase database;
  memset(&database, 0, sizeof(database));
  ASSERT_EQ(driver.DatabaseNew(&database, nullptr), ADBC_STATUS_OK);
  database.private_driver = &driver;
  Handle<AdbcDatabase> database_handle(&database);
  ASSERT_EQ(driver.DatabaseInit(&database, nullptr), ADBC_STATUS_OK);

  std::vector<char> opt_string;
  std::vector<uint8_t> opt_bytes;
  size_t opt_size = 0;
  int64_t opt_int = 0;
  double opt_double = 0;

  // Check return codes without an error pointer for non-existent keys
  ASSERT_EQ(driver.DatabaseGetOption(&database, "key_that_does_not_exist", nullptr,
                                     &opt_size, nullptr),
            ADBC_STATUS_NOT_FOUND);
  ASSERT_EQ(driver.DatabaseGetOptionBytes(&database, "key_that_does_not_exist", nullptr,
                                          &opt_size, nullptr),
            ADBC_STATUS_NOT_FOUND);
  ASSERT_EQ(driver.DatabaseGetOptionInt(&database, "key_that_does_not_exist", &opt_int,
                                        nullptr),
            ADBC_STATUS_NOT_FOUND);
  ASSERT_EQ(driver.DatabaseGetOptionDouble(&database, "key_that_does_not_exist",
                                           &opt_double, nullptr),
            ADBC_STATUS_NOT_FOUND);

  // Check set/get for string
  ASSERT_EQ(driver.DatabaseSetOption(&database, "key_string", "value_string", nullptr),
            ADBC_STATUS_OK);
  opt_size = 0;
  ASSERT_EQ(
      driver.DatabaseGetOption(&database, "key_string", nullptr, &opt_size, nullptr),
      ADBC_STATUS_OK);
  ASSERT_EQ(opt_size, strlen("value_string") + 1);
  opt_string.resize(opt_size);
  ASSERT_EQ(driver.DatabaseGetOption(&database, "key_string", opt_string.data(),
                                     &opt_size, nullptr),
            ADBC_STATUS_OK);

  // Check set/get for bytes
  const uint8_t test_bytes[] = {0x01, 0x02, 0x03};
  ASSERT_EQ(driver.DatabaseSetOptionBytes(&database, "key_bytes", test_bytes,
                                          sizeof(test_bytes), nullptr),
            ADBC_STATUS_OK);
  opt_size = 0;
  ASSERT_EQ(
      driver.DatabaseGetOptionBytes(&database, "key_bytes", nullptr, &opt_size, nullptr),
      ADBC_STATUS_OK);
  ASSERT_EQ(opt_size, sizeof(test_bytes));
  opt_bytes.resize(opt_size);
  ASSERT_EQ(driver.DatabaseGetOptionBytes(&database, "key_bytes", opt_bytes.data(),
                                          &opt_size, nullptr),
            ADBC_STATUS_OK);

  // Check set/get for int
  ASSERT_EQ(driver.DatabaseSetOptionInt(&database, "key_int", 1234, nullptr),
            ADBC_STATUS_OK);
  ASSERT_EQ(driver.DatabaseGetOptionInt(&database, "key_int", &opt_int, nullptr),
            ADBC_STATUS_OK);
  ASSERT_EQ(opt_int, 1234);

  // Check set/get for double
  ASSERT_EQ(driver.DatabaseSetOptionDouble(&database, "key_double", 1234.5, nullptr),
            ADBC_STATUS_OK);
  ASSERT_EQ(driver.DatabaseGetOptionDouble(&database, "key_double", &opt_double, nullptr),
            ADBC_STATUS_OK);
  ASSERT_EQ(opt_double, 1234.5);

  // Check error code for getting a key of an incorrect type
  opt_size = 0;
  ASSERT_EQ(driver.DatabaseGetOption(&database, "key_bytes", nullptr, &opt_size, nullptr),
            ADBC_STATUS_NOT_FOUND);
  ASSERT_EQ(
      driver.DatabaseGetOptionBytes(&database, "key_string", nullptr, &opt_size, nullptr),
      ADBC_STATUS_NOT_FOUND);
  ASSERT_EQ(driver.DatabaseGetOptionInt(&database, "key_bytes", &opt_int, nullptr),
            ADBC_STATUS_NOT_FOUND);
  ASSERT_EQ(driver.DatabaseGetOptionDouble(&database, "key_bytes", &opt_double, nullptr),
            ADBC_STATUS_NOT_FOUND);
}
