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

#include "driver_example.h"

#include "arrow-adbc/adbc_driver_manager.h"
#include "gtest/gtest.h"

TEST(DriverExample, TestLifecycle) {
  struct AdbcDriver driver;
  ASSERT_EQ(AdbcLoadDriverFromInitFunc(&ExampleDriverInitFunc, ADBC_VERSION_1_1_0,
                                       &driver, nullptr),
            ADBC_STATUS_OK);

  struct AdbcDatabase database;
  ASSERT_EQ(AdbcDatabaseNew(&database, nullptr), ADBC_STATUS_OK);
  ASSERT_EQ(AdbcDatabaseInit(&database, nullptr), ADBC_STATUS_OK);

  struct AdbcConnection connection;
  ASSERT_EQ(AdbcConnectionNew(&connection, nullptr), ADBC_STATUS_OK);
  ASSERT_EQ(AdbcConnectionInit(&connection, &database, nullptr), ADBC_STATUS_OK);

  struct AdbcStatement statement;
  ASSERT_EQ(AdbcStatementNew(&connection, &statement, nullptr), ADBC_STATUS_OK);

  ASSERT_EQ(AdbcStatementRelease(&statement, nullptr), ADBC_STATUS_OK);
  ASSERT_EQ(AdbcConnectionRelease(&connection, nullptr), ADBC_STATUS_OK);
  ASSERT_EQ(AdbcDatabaseRelease(&database, nullptr), ADBC_STATUS_OK);

  ASSERT_EQ(driver.release(&driver, nullptr), ADBC_STATUS_OK);
}
