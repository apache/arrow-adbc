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

#include "error.h"

namespace adbcpq {

TEST(PostgresErrorTest, ClassifySqlState) {
  struct Case {
    const char* sqlstate;
    AdbcStatusCode status;
  };

  const Case kCases[] = {
      {nullptr, ADBC_STATUS_IO},
      {"57014", ADBC_STATUS_CANCELLED},
      {"0A000", ADBC_STATUS_NOT_IMPLEMENTED},
      {"22012", ADBC_STATUS_INVALID_DATA},
      {"22P02", ADBC_STATUS_INVALID_DATA},
      {"23505", ADBC_STATUS_INTEGRITY},
      {"23503", ADBC_STATUS_INTEGRITY},
      {"28P01", ADBC_STATUS_UNAUTHENTICATED},
      {"28000", ADBC_STATUS_UNAUTHENTICATED},
      {"42501", ADBC_STATUS_UNAUTHORIZED},
      {"42P01", ADBC_STATUS_NOT_FOUND},
      {"42P02", ADBC_STATUS_NOT_FOUND},
      {"42703", ADBC_STATUS_NOT_FOUND},
      {"42602", ADBC_STATUS_NOT_FOUND},
      {"3D000", ADBC_STATUS_NOT_FOUND},
      {"58P01", ADBC_STATUS_NOT_FOUND},
      {"42701", ADBC_STATUS_ALREADY_EXISTS},
      {"42P03", ADBC_STATUS_ALREADY_EXISTS},
      {"42P07", ADBC_STATUS_ALREADY_EXISTS},
      {"42P05", ADBC_STATUS_ALREADY_EXISTS},
      {"42712", ADBC_STATUS_ALREADY_EXISTS},
      {"42710", ADBC_STATUS_ALREADY_EXISTS},
      {"58P02", ADBC_STATUS_ALREADY_EXISTS},
      {"25P02", ADBC_STATUS_INVALID_STATE},
      {"55P03", ADBC_STATUS_INVALID_STATE},
      {"XX000", ADBC_STATUS_INTERNAL},
      {"42601", ADBC_STATUS_INVALID_ARGUMENT},
      {"08006", ADBC_STATUS_IO},
      {"53000", ADBC_STATUS_IO},
      {"ZZ999", ADBC_STATUS_IO},
  };

  for (const auto& test_case : kCases) {
    EXPECT_EQ(test_case.status, ClassifySqlState(test_case.sqlstate))
        << (test_case.sqlstate ? test_case.sqlstate : "null");
  }
}

}  // namespace adbcpq
