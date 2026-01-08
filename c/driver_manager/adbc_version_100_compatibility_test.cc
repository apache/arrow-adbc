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

#include "adbc_version_100.h"
#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"
#include "validation/adbc_validation_util.h"

namespace adbc {

using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;

class AdbcVersion : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));
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

TEST_F(AdbcVersion, StructSize) {
  ASSERT_EQ(sizeof(AdbcErrorVersion100), ADBC_ERROR_1_0_0_SIZE);
  ASSERT_EQ(sizeof(AdbcError), ADBC_ERROR_1_1_0_SIZE);
  ASSERT_EQ(sizeof(AdbcError), ADBC_ERROR_1_2_0_SIZE);

  ASSERT_EQ(sizeof(AdbcDriverVersion100), ADBC_DRIVER_1_0_0_SIZE);
  ASSERT_EQ(offsetof(struct AdbcDriver, StatementNextResultSet), ADBC_DRIVER_1_1_0_SIZE);
  ASSERT_EQ(sizeof(AdbcDriver), ADBC_DRIVER_1_2_0_SIZE);
}

// Initialize a version 1.0.0 driver with the version 1.1.0 driver struct.
TEST_F(AdbcVersion, OldDriverNewLayout) {
  ASSERT_THAT(Version100DriverInit(ADBC_VERSION_1_1_0, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));

  ASSERT_THAT(Version100DriverInit(ADBC_VERSION_1_0_0, &driver, &error),
              IsOkStatus(&error));
}

// Initialize a version 1.0.0 driver with the new driver manager/new version.
TEST_F(AdbcVersion, OldDriverNewManager) {
  ASSERT_THAT(AdbcLoadDriverFromInitFunc(&Version100DriverInit, ADBC_VERSION_1_1_0,
                                         &driver, &error),
              IsOkStatus(&error));

  EXPECT_NE(driver.ErrorGetDetailCount, nullptr);
  EXPECT_NE(driver.ErrorGetDetail, nullptr);

  EXPECT_NE(driver.DatabaseGetOption, nullptr);
  EXPECT_NE(driver.DatabaseGetOptionBytes, nullptr);
  EXPECT_NE(driver.DatabaseGetOptionDouble, nullptr);
  EXPECT_NE(driver.DatabaseGetOptionInt, nullptr);
  EXPECT_NE(driver.DatabaseSetOptionInt, nullptr);
  EXPECT_NE(driver.DatabaseSetOptionDouble, nullptr);

  EXPECT_NE(driver.ConnectionCancel, nullptr);
  EXPECT_NE(driver.ConnectionGetOption, nullptr);
  EXPECT_NE(driver.ConnectionGetOptionBytes, nullptr);
  EXPECT_NE(driver.ConnectionGetOptionDouble, nullptr);
  EXPECT_NE(driver.ConnectionGetOptionInt, nullptr);
  EXPECT_NE(driver.ConnectionSetOptionInt, nullptr);
  EXPECT_NE(driver.ConnectionSetOptionDouble, nullptr);

  EXPECT_NE(driver.StatementCancel, nullptr);
  EXPECT_NE(driver.StatementExecuteSchema, nullptr);
  EXPECT_NE(driver.StatementGetOption, nullptr);
  EXPECT_NE(driver.StatementGetOptionBytes, nullptr);
  EXPECT_NE(driver.StatementGetOptionDouble, nullptr);
  EXPECT_NE(driver.StatementGetOptionInt, nullptr);
  EXPECT_NE(driver.StatementSetOptionInt, nullptr);
  EXPECT_NE(driver.StatementSetOptionDouble, nullptr);
}

// N.B. see postgresql_test.cc for backwards compatibility test of AdbcError
// N.B. see postgresql_test.cc for backwards compatibility test of AdbcDriver

}  // namespace adbc
