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

#include "adbc_validation.h"

#include <cstring>

#include <arrow-adbc/adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "adbc_validation_util.h"

namespace adbc_validation {
void DatabaseTest::SetUpTest() {
  std::memset(&error, 0, sizeof(error));
  std::memset(&database, 0, sizeof(database));
}

void DatabaseTest::TearDownTest() {
  if (database.private_data) {
    ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  }
  if (error.release) {
    error.release(&error);
  }
}

void DatabaseTest::TestNewInit() {
  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->SetupDatabase(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));
  ASSERT_NE(nullptr, database.private_data);
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  ASSERT_EQ(nullptr, database.private_data);

  ASSERT_THAT(AdbcDatabaseRelease(&database, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
}

void DatabaseTest::TestRelease() {
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));

  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  ASSERT_EQ(nullptr, database.private_data);
}
}  // namespace adbc_validation
