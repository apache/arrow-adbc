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

class Postgres : public ::testing::Test {
 protected:
  void SetUp() override {
    std::memset(&ctx, 0, sizeof(ctx));
    ctx.setup_database = SetupDatabase;
  }

  void TearDown() override {
    ASSERT_EQ(ctx.failed, 0);
    ASSERT_EQ(ctx.total, ctx.passed);
  }

  struct AdbcValidateTestContext ctx;

  static AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                                      struct AdbcError* error) {
    const char* uri = std::getenv("ADBC_POSTGRES_TEST_URI");
    if (!uri) {
      ADD_FAILURE() << "Must provide env var ADBC_POSTGRES_TEST_URI";
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return AdbcDatabaseSetOption(database, "uri", uri, error);
  }
};

TEST_F(Postgres, ValidationDatabase) { AdbcValidateDatabaseNewRelease(&ctx); }

TEST_F(Postgres, ValidationConnectionNewRelease) {
  AdbcValidateConnectionNewRelease(&ctx);
}

TEST_F(Postgres, ValidationConnectionAutocommit) {
  AdbcValidateConnectionAutocommit(&ctx);
}

TEST_F(Postgres, ValidationStatementNewRelease) { AdbcValidateStatementNewRelease(&ctx); }

TEST_F(Postgres, ValidationStatementSqlExecute) { AdbcValidateStatementSqlExecute(&ctx); }

TEST_F(Postgres, ValidationStatementSqlIngest) { AdbcValidateStatementSqlIngest(&ctx); }

TEST_F(Postgres, ValidationStatementSqlPrepare) {
  GTEST_SKIP() << "Not yet implemented";
  AdbcValidateStatementSqlPrepare(&ctx);
}
