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

#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <adbc.h>

#define ADBCV_STRINGIFY(s) #s
const char* AdbcValidateStatusCodeMessage(AdbcStatusCode code) {
#define STRINGIFY_VALUE(s) ADBCV_STRINGIFY(s)
#define CASE(CONSTANT)         \
  case ADBC_STATUS_##CONSTANT: \
    return #CONSTANT " (" STRINGIFY_VALUE(ADBC_STATUS_##CONSTANT) ")";

  switch (code) {
    CASE(OK);
    CASE(UNKNOWN);
    CASE(NOT_IMPLEMENTED);
    CASE(NOT_FOUND);
    CASE(ALREADY_EXISTS);
    CASE(INVALID_ARGUMENT);
    CASE(INVALID_STATE);
    CASE(INVALID_DATA);
    CASE(INTEGRITY);
    CASE(INTERNAL);
    CASE(IO);
    CASE(CANCELLED);
    CASE(TIMEOUT);
    CASE(UNAUTHENTICATED);
    CASE(UNAUTHORIZED);
    default:
      return "(invalid code)";
  }
#undef CASE
#undef STRINGIFY_VALUE
}

void AdbcValidateBeginCase(struct AdbcValidateTestContext* ctx, const char* suite,
                           const char* test_case) {
  printf("-- %s: %s\n", suite, test_case);
}

void AdbcValidateBeginAssert(struct AdbcValidateTestContext* ctx, const char* fmt, ...) {
  ctx->total++;
  printf("   ");
  va_list args;
  va_start(args, fmt);
  vprintf(fmt, args);
  va_end(args);
  printf(": ");
  fflush(stdout);
}

void AdbcValidatePass(struct AdbcValidateTestContext* ctx) {
  ctx->passed++;
  printf("pass\n");
}

void AdbcValidateFail(struct AdbcValidateTestContext* ctx, const char* file, int lineno,
                      struct AdbcError* error) {
  ctx->failed++;
  printf("FAIL\n");
  printf("%s:%d\n", file, lineno);
  if (error && error->release) {
    printf("%s\n", error->message);
    error->release(error);
  }
}

#define ADBCV_ASSERT_FAILS_WITH(STATUS, ERROR, EXPR)                            \
  AdbcValidateBeginAssert(adbc_context, "%s == %s", #EXPR,                      \
                          AdbcValidateStatusCodeMessage(ADBC_STATUS_##STATUS)); \
  if (ADBC_STATUS_##STATUS != (EXPR)) {                                         \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, ERROR);                  \
    return;                                                                     \
  }                                                                             \
  AdbcValidatePass(adbc_context);
#define ADBCV_ASSERT_OK(ERROR, EXPR)                                      \
  AdbcValidateBeginAssert(adbc_context, "%s == %s", #EXPR,                \
                          AdbcValidateStatusCodeMessage(ADBC_STATUS_OK)); \
  if (ADBC_STATUS_OK != (EXPR)) {                                         \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, ERROR);            \
    return;                                                               \
  }                                                                       \
  AdbcValidatePass(adbc_context);
#define ADBCV_ASSERT_EQ(EXPECTED, ACTUAL)                                  \
  AdbcValidateBeginAssert(adbc_context, "%s == %s: ", #ACTUAL, #EXPECTED); \
  if ((EXPECTED) != (ACTUAL)) {                                            \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, NULL);              \
    return;                                                                \
  }                                                                        \
  AdbcValidatePass(adbc_context);
#define ADBCV_ASSERT_NE(EXPECTED, ACTUAL)                                  \
  AdbcValidateBeginAssert(adbc_context, "%s == %s: ", #ACTUAL, #EXPECTED); \
  if ((EXPECTED) == (ACTUAL)) {                                            \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, NULL);              \
    return;                                                                \
  }                                                                        \
  AdbcValidatePass(adbc_context);

void AdbcValidateDatabaseNewRelease(struct AdbcValidateTestContext* adbc_context) {
  struct AdbcError error;
  struct AdbcDatabase database;
  memset(&error, 0, sizeof(error));
  memset(&database, 0, sizeof(database));

  AdbcValidateBeginCase(adbc_context, "Database", "new then release");
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcDatabaseRelease(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  ADBCV_ASSERT_EQ(NULL, database.private_data);

  AdbcValidateBeginCase(adbc_context, "Database", "new, init, release");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  if (adbc_context->setup_database) {
    ADBCV_ASSERT_OK(&error, adbc_context->setup_database(&database, &error));
  }
  ADBCV_ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  ADBCV_ASSERT_EQ(NULL, database.private_data);
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error, AdbcDatabaseRelease(&database, &error));
}

void AdbcValidateConnectionNewRelease(struct AdbcValidateTestContext* adbc_context) {
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  memset(&error, 0, sizeof(error));
  memset(&database, 0, sizeof(database));
  memset(&connection, 0, sizeof(connection));

  AdbcValidateBeginCase(adbc_context, "Connection", "setup");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  if (adbc_context->setup_database) {
    ADBCV_ASSERT_OK(&error, adbc_context->setup_database(&database, &error));
  }
  ADBCV_ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcConnectionRelease(&connection, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "new then release");
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_EQ(NULL, connection.private_data);

  AdbcValidateBeginCase(adbc_context, "Connection", "new, init, release");
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_EQ(NULL, connection.private_data);
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcConnectionRelease(&connection, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "two concurrent connections");
  struct AdbcConnection connection2;
  memset(&connection2, 0, sizeof(connection2));
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection2, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection2, &database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection2, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "Teardown");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) error.release(&error);
}

void AdbcValidateConnectionAutocommit(struct AdbcValidateTestContext* adbc_context) {
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  memset(&error, 0, sizeof(error));
  memset(&database, 0, sizeof(database));
  memset(&connection, 0, sizeof(connection));

  AdbcValidateBeginCase(adbc_context, "Connection", "setup");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  if (adbc_context->setup_database) {
    ADBCV_ASSERT_OK(&error, adbc_context->setup_database(&database, &error));
  }
  ADBCV_ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  AdbcValidateBeginCase(adbc_context, "Connection",
                        "commit fails with autocommit (on by default)");
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcConnectionCommit(&connection, &error));

  AdbcValidateBeginCase(adbc_context, "Connection",
                        "rollback fails with autocommit (on by default)");
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcConnectionRollback(&connection, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "enable autocommit");
  ADBCV_ASSERT_OK(&error,
                  AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                          ADBC_OPTION_VALUE_ENABLED, &error));
  AdbcValidateBeginCase(adbc_context, "Connection", "disable autocommit");
  ADBCV_ASSERT_OK(&error,
                  AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                          ADBC_OPTION_VALUE_DISABLED, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "commit/rollback succeed");
  ADBCV_ASSERT_OK(&error, AdbcConnectionCommit(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionRollback(&connection, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "commit/rollback are idempotent");
  ADBCV_ASSERT_OK(&error, AdbcConnectionCommit(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionCommit(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionRollback(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionRollback(&connection, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "enable autocommit");
  ADBCV_ASSERT_OK(&error,
                  AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                          ADBC_OPTION_VALUE_ENABLED, &error));
  AdbcValidateBeginCase(adbc_context, "Connection",
                        "commit/rollback fail with autocommit");
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcConnectionCommit(&connection, &error));
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcConnectionRollback(&connection, &error));

  AdbcValidateBeginCase(adbc_context, "Connection", "Teardown");
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) error.release(&error);
}

void AdbcValidateStatementNewRelease(struct AdbcValidateTestContext* adbc_context) {
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  struct AdbcStatement statement;
  memset(&error, 0, sizeof(error));
  memset(&database, 0, sizeof(database));
  memset(&connection, 0, sizeof(connection));
  memset(&statement, 0, sizeof(statement));

  AdbcValidateBeginCase(adbc_context, "StatementNewRelease", "setup");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  if (adbc_context->setup_database) {
    ADBCV_ASSERT_OK(&error, adbc_context->setup_database(&database, &error));
  }
  ADBCV_ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  AdbcValidateBeginCase(adbc_context, "StatementNewRelease", "new then release");
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcStatementRelease(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementNewRelease", "new, execute, release");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcStatementExecute(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementNewRelease", "new, get stream, release");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  struct ArrowArrayStream out;
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcStatementGetStream(&statement, &out, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementNewRelease", "new, prepare, release");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcStatementPrepare(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementNewRelease", "Teardown");
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) error.release(&error);
}

void AdbcValidateStatementSqlExecute(struct AdbcValidateTestContext* adbc_context) {
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  struct AdbcStatement statement;
  struct ArrowArrayStream out;
  memset(&error, 0, sizeof(error));
  memset(&database, 0, sizeof(database));
  memset(&connection, 0, sizeof(connection));
  memset(&statement, 0, sizeof(statement));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "setup");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  if (adbc_context->setup_database) {
    ADBCV_ASSERT_OK(&error, adbc_context->setup_database(&database, &error));
  }
  ADBCV_ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "execute");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementExecute(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementGetStream(&statement, &out, &error));
  ADBCV_ASSERT_NE(NULL, out.release);
  out.release(&out);
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "Teardown");
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) error.release(&error);
}

void AdbcValidateStatementSqlPrepare(struct AdbcValidateTestContext* adbc_context) {
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  struct AdbcStatement statement;
  struct ArrowArrayStream out;
  memset(&error, 0, sizeof(error));
  memset(&database, 0, sizeof(database));
  memset(&connection, 0, sizeof(connection));
  memset(&statement, 0, sizeof(statement));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "setup");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  if (adbc_context->setup_database) {
    ADBCV_ASSERT_OK(&error, adbc_context->setup_database(&database, &error));
  }
  ADBCV_ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "prepare");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementExecute(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementGetStream(&statement, &out, &error));
  ADBCV_ASSERT_NE(NULL, out.release);
  out.release(&out);
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "prepare without execute");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));
  ADBCV_ASSERT_FAILS_WITH(INVALID_STATE, &error,
                          AdbcStatementGetStream(&statement, &out, &error));
  ADBCV_ASSERT_EQ(NULL, out.release);
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "Teardown");
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) error.release(&error);
}
