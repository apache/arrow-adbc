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

#include <cmath>
#include <limits>
#include <locale>
#include <memory>
#include <string>
#include <vector>

#include "adbc_version_100.h"
#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"
#include "validation/adbc_validation_util.h"

namespace adbc {

using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;

std::vector<std::string> option_values;
AdbcStatusCode option_status = ADBC_STATUS_OK;

template <typename Object>
AdbcStatusCode CaptureOption(Object*, const char* key, const char* value, AdbcError*) {
  EXPECT_STREQ(key, "option");
  option_values.emplace_back(value);
  return option_status;
}

AdbcStatusCode LegacyOptionDriverInit(int version, void* raw_driver, AdbcError* error) {
  auto status = Version100DriverInit(version, raw_driver, error);
  if (status != ADBC_STATUS_OK) return status;

  auto* driver = static_cast<AdbcDriver*>(raw_driver);
  driver->DatabaseSetOption = CaptureOption<AdbcDatabase>;
  driver->ConnectionSetOption = CaptureOption<AdbcConnection>;
  driver->StatementSetOption = CaptureOption<AdbcStatement>;
  return ADBC_STATUS_OK;
}

AdbcStatusCode Version110OptionDriverInit(int, void* raw_driver, AdbcError* error) {
  return LegacyOptionDriverInit(ADBC_VERSION_1_0_0, raw_driver, error);
}

AdbcStatusCode NativeStatementSetOptionInt(AdbcStatement*, const char*, int64_t,
                                           AdbcError*) {
  return ADBC_STATUS_CANCELLED;
}

AdbcStatusCode Version110NativeOptionDriverInit(int version, void* raw_driver,
                                                AdbcError* error) {
  auto status = Version110OptionDriverInit(version, raw_driver, error);
  static_cast<AdbcDriver*>(raw_driver)->StatementSetOptionInt =
      NativeStatementSetOptionInt;
  return status;
}

struct CommaDecimal : std::numpunct<char> {
  char do_decimal_point() const override { return ','; }
};

class ScopedCommaLocale {
 public:
  ScopedCommaLocale() : previous_(std::locale()) {
    std::locale::global(std::locale(previous_, new CommaDecimal));
  }
  ~ScopedCommaLocale() { std::locale::global(previous_); }

 private:
  std::locale previous_;
};

class AdbcVersion : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&driver, 0, sizeof(driver));
    std::memset(&error, 0, sizeof(error));
    option_values.clear();
    option_status = ADBC_STATUS_OK;
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

  ASSERT_EQ(sizeof(AdbcDriverVersion100), ADBC_DRIVER_1_0_0_SIZE);
  ASSERT_EQ(sizeof(AdbcDriver), ADBC_DRIVER_1_1_0_SIZE);
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

TEST_F(AdbcVersion, OldDriverNumericSetters) {
  ASSERT_THAT(AdbcLoadDriverFromInitFunc(&LegacyOptionDriverInit, ADBC_VERSION_1_1_0,
                                         &driver, &error),
              IsOkStatus(&error));

  AdbcDatabase database = {nullptr, &driver};
  AdbcConnection connection = {nullptr, &driver};
  AdbcStatement statement = {nullptr, &driver};
  ASSERT_THAT(driver.DatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(driver.ConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(driver.StatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ScopedCommaLocale locale;
  for (int64_t value : {std::numeric_limits<int64_t>::min(), int64_t{0},
                        std::numeric_limits<int64_t>::max()}) {
    ASSERT_THAT(AdbcDatabaseSetOptionInt(&database, "option", value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionSetOptionInt(&connection, "option", value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOptionInt(&statement, "option", value, &error),
                IsOkStatus(&error));
    for (size_t i = option_values.size() - 3; i < option_values.size(); ++i) {
      EXPECT_EQ(std::to_string(value), option_values[i]);
    }
  }

  for (double value : {1.2345678901234567, 1e-12, -0.0, 1e200}) {
    ASSERT_THAT(AdbcDatabaseSetOptionDouble(&database, "option", value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionSetOptionDouble(&connection, "option", value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOptionDouble(&statement, "option", value, &error),
                IsOkStatus(&error));
    for (size_t i = option_values.size() - 3; i < option_values.size(); ++i) {
      EXPECT_EQ(std::string::npos, option_values[i].find(','));
      EXPECT_EQ(value, std::stod(option_values[i]));
      EXPECT_EQ(std::signbit(value), std::signbit(std::stod(option_values[i])));
    }
  }

  EXPECT_THAT(driver.StatementRelease(&statement, &error), IsOkStatus(&error));
  EXPECT_THAT(driver.ConnectionRelease(&connection, &error), IsOkStatus(&error));
  EXPECT_THAT(driver.DatabaseRelease(&database, &error), IsOkStatus(&error));
}

TEST_F(AdbcVersion, OldDriverQueuedOptionsAndErrorStatus) {
  AdbcDatabase database = {};
  AdbcConnection connection = {};
  AdbcStatement statement = {};
  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcDriverManagerDatabaseSetInitFunc(&database, LegacyOptionDriverInit, &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseSetOptionInt(&database, "option", 42, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOptionDouble(&connection, "option", 1e-12, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  option_status = ADBC_STATUS_INVALID_ARGUMENT;
  EXPECT_THAT(AdbcStatementSetOptionInt(&statement, "option", 1, &error),
              IsStatus(option_status, &error));
  ASSERT_EQ(3U, option_values.size());
  EXPECT_EQ("42", option_values[0]);
  EXPECT_EQ(1e-12, std::stod(option_values[1]));
  EXPECT_EQ("1", option_values[2]);

  option_status = ADBC_STATUS_OK;
  EXPECT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
  EXPECT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  EXPECT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
}

TEST_F(AdbcVersion, Version110DriverDoesNotUseFallback) {
  ASSERT_THAT(AdbcLoadDriverFromInitFunc(&Version110OptionDriverInit, ADBC_VERSION_1_1_0,
                                         &driver, &error),
              IsOkStatus(&error));
  AdbcStatement statement = {nullptr, &driver};
  EXPECT_THAT(AdbcStatementSetOptionInt(&statement, "option", 1, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  EXPECT_TRUE(option_values.empty());
}

TEST_F(AdbcVersion, OldDriverBytesRemainUnsupported) {
  ASSERT_THAT(AdbcLoadDriverFromInitFunc(&LegacyOptionDriverInit, ADBC_VERSION_1_1_0,
                                         &driver, &error),
              IsOkStatus(&error));
  AdbcStatement statement = {nullptr, &driver};
  const uint8_t value[] = {'a', 0, 'b'};
  EXPECT_THAT(
      AdbcStatementSetOptionBytes(&statement, "option", value, sizeof(value), &error),
      IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
  EXPECT_TRUE(option_values.empty());
}

TEST_F(AdbcVersion, Version110NativeNumericSetterIsPreserved) {
  ASSERT_THAT(AdbcLoadDriverFromInitFunc(&Version110NativeOptionDriverInit,
                                         ADBC_VERSION_1_1_0, &driver, &error),
              IsOkStatus(&error));
  AdbcStatement statement = {nullptr, &driver};
  EXPECT_THAT(AdbcStatementSetOptionInt(&statement, "option", 1, &error),
              IsStatus(ADBC_STATUS_CANCELLED, &error));
  EXPECT_TRUE(option_values.empty());
}

// N.B. see postgresql_test.cc for backwards compatibility test of AdbcError
// N.B. see postgresql_test.cc for backwards compatibility test of AdbcDriver

}  // namespace adbc
