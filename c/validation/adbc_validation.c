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
#include <nanoarrow.h>

#define ADBCV_STRINGIFY(s) #s
const char* AdbcValidateStatusCodeMessage(AdbcStatusCode code) {
#define STRINGIFY_VALUE(s) ADBCV_STRINGIFY(s)
#define CASE(CONSTANT)         \
  case ADBC_STATUS_##CONSTANT: \
    return STRINGIFY_VALUE(ADBC_STATUS_##CONSTANT) " (" #CONSTANT ")";

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
  printf("\n%s:%d: FAIL\n", file, lineno);
  if (error && error->release) {
    printf("%s\n", error->message);
    error->release(error);
  }
}

int AdbcValidationIsSet(struct ArrowArray* array, int64_t i) {
  // TODO: unions
  if (array->n_buffers == 0) return 0;
  if (!array->buffers[0]) return 1;
  return ArrowBitGet((const uint8_t*)array->buffers[0], i);
}

#define ADBCV_CONCAT(a, b) a##b
#define ADBCV_NAME(a, b) ADBCV_CONCAT(a, b)
#define ADBCV_ASSERT_FAILS_WITH_IMPL(STATUS, ERROR, NAME, EXPR)                 \
  AdbcValidateBeginAssert(adbc_context, "%s == %s", #EXPR,                      \
                          AdbcValidateStatusCodeMessage(ADBC_STATUS_##STATUS)); \
  AdbcStatusCode NAME = (EXPR);                                                 \
  if (ADBC_STATUS_##STATUS != NAME) {                                           \
    printf("\nActual value: %s\n", AdbcValidateStatusCodeMessage(NAME));        \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, ERROR);                  \
    return;                                                                     \
  }                                                                             \
  AdbcValidatePass(adbc_context);
#define ADBCV_ASSERT_FAILS_WITH(STATUS, ERROR, EXPR) \
  ADBCV_ASSERT_FAILS_WITH_IMPL(STATUS, ERROR, ADBCV_NAME(adbc_status_, __COUNTER__), EXPR)
#define ADBCV_ASSERT_OK(ERROR, EXPR) ADBCV_ASSERT_FAILS_WITH(OK, ERROR, EXPR)
#define ADBCV_ASSERT_EQ(EXPECTED, ACTUAL)                                \
  AdbcValidateBeginAssert(adbc_context, "%s == %s", #ACTUAL, #EXPECTED); \
  if ((EXPECTED) != (ACTUAL)) {                                          \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, NULL);            \
    return;                                                              \
  }                                                                      \
  AdbcValidatePass(adbc_context);
#define ADBCV_ASSERT_NE(EXPECTED, ACTUAL)                                \
  AdbcValidateBeginAssert(adbc_context, "%s == %s", #ACTUAL, #EXPECTED); \
  if ((EXPECTED) == (ACTUAL)) {                                          \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, NULL);            \
    return;                                                              \
  }                                                                      \
  AdbcValidatePass(adbc_context);
#define ADBCV_ASSERT_TRUE(ACTUAL)                               \
  AdbcValidateBeginAssert(adbc_context, "%s is true", #ACTUAL); \
  if (!(ACTUAL)) {                                              \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, NULL);   \
    return;                                                     \
  }                                                             \
  AdbcValidatePass(adbc_context);
#define ADBCV_ASSERT_FALSE(ACTUAL)                               \
  AdbcValidateBeginAssert(adbc_context, "%s is false", #ACTUAL); \
  if (ACTUAL) {                                                  \
    AdbcValidateFail(adbc_context, __FILE__, __LINE__, NULL);    \
    return;                                                      \
  }                                                              \
  AdbcValidatePass(adbc_context);

#define NA_ASSERT_OK_IMPL(ERROR_NAME, EXPR)                       \
  do {                                                            \
    AdbcValidateBeginAssert(adbc_context, "%s is OK (0)", #EXPR); \
    ArrowErrorCode ERROR_NAME = (EXPR);                           \
    if (ERROR_NAME) {                                             \
      AdbcValidateFail(adbc_context, __FILE__, __LINE__, NULL);   \
      return;                                                     \
    }                                                             \
    AdbcValidatePass(adbc_context);                               \
  } while (0)

#define NA_ASSERT_OK(EXPR) NA_ASSERT_OK_IMPL(ADBCV_NAME(na_status_, __COUNTER__), EXPR)

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
  ADBCV_ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementExecute(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementGetStream(&statement, &out, &error));
  ADBCV_ASSERT_NE(NULL, out.release);

  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;
  ADBCV_ASSERT_EQ(0, out.get_schema(&out, &schema));
  ADBCV_ASSERT_EQ(1, schema.n_children);
  ADBCV_ASSERT_EQ(0, ArrowSchemaViewInit(&schema_view, schema.children[0], NULL));
  ADBCV_ASSERT_EQ(NANOARROW_TYPE_INT64, schema_view.data_type);

  struct ArrowArray array;
  ADBCV_ASSERT_EQ(0, out.get_next(&out, &array));
  ADBCV_ASSERT_NE(NULL, array.release);

  ADBCV_ASSERT_TRUE(AdbcValidationIsSet(array.children[0], 0));
  ADBCV_ASSERT_EQ(42, ((int64_t*)array.children[0]->buffers[1])[0]);

  array.release(&array);
  ADBCV_ASSERT_EQ(0, out.get_next(&out, &array));
  ADBCV_ASSERT_EQ(NULL, array.release);

  schema.release(&schema);
  out.release(&out);
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSql", "teardown");
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) error.release(&error);
}

void AdbcValidateStatementSqlIngest(struct AdbcValidateTestContext* adbc_context) {
  struct AdbcError error;
  struct AdbcDatabase database;
  struct AdbcConnection connection;
  struct AdbcStatement statement;
  struct ArrowArrayStream out;
  memset(&error, 0, sizeof(error));
  memset(&database, 0, sizeof(database));
  memset(&connection, 0, sizeof(connection));
  memset(&statement, 0, sizeof(statement));

  AdbcValidateBeginCase(adbc_context, "StatementSqlIngest", "setup");
  ADBCV_ASSERT_OK(&error, AdbcDatabaseNew(&database, &error));
  if (adbc_context->setup_database) {
    ADBCV_ASSERT_OK(&error, adbc_context->setup_database(&database, &error));
  }
  ADBCV_ASSERT_OK(&error, AdbcDatabaseInit(&database, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionNew(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcConnectionInit(&connection, &database, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSqlIngest", "ingest int64");

  struct ArrowSchema export_schema;
  NA_ASSERT_OK(ArrowSchemaInit(&export_schema, NANOARROW_TYPE_STRUCT));
  NA_ASSERT_OK(ArrowSchemaAllocateChildren(&export_schema, 1));
  NA_ASSERT_OK(ArrowSchemaInit(export_schema.children[0], NANOARROW_TYPE_INT64));
  NA_ASSERT_OK(ArrowSchemaSetName(export_schema.children[0], "int64"));

  struct ArrowArray export_array;
  NA_ASSERT_OK(ArrowArrayInit(&export_array, NANOARROW_TYPE_STRUCT));
  NA_ASSERT_OK(ArrowArrayAllocateChildren(&export_array, 1));
  NA_ASSERT_OK(ArrowArrayInit(export_array.children[0], NANOARROW_TYPE_INT64));

  struct ArrowBitmap* bitmap = ArrowArrayValidityBitmap(export_array.children[0]);
  struct ArrowBuffer* buffer = ArrowArrayBuffer(export_array.children[0], 1);
  NA_ASSERT_OK(ArrowBitmapReserve(bitmap, 5));
  NA_ASSERT_OK(ArrowBufferReserve(buffer, 5 * sizeof(int64_t)));
  ArrowBitmapAppendInt8Unsafe(bitmap, (int8_t[]){1, 1, 0, 0, 1}, 5);
  NA_ASSERT_OK(ArrowBufferAppendInt64(buffer, 16));
  NA_ASSERT_OK(ArrowBufferAppendInt64(buffer, -1));
  NA_ASSERT_OK(ArrowBufferAppendInt64(buffer, 0));
  NA_ASSERT_OK(ArrowBufferAppendInt64(buffer, 0));
  NA_ASSERT_OK(ArrowBufferAppendInt64(buffer, 42));
  NA_ASSERT_OK(ArrowArrayFinishBuilding(export_array.children[0], 0));
  NA_ASSERT_OK(ArrowArrayFinishBuilding(&export_array, 0));
  export_array.children[0]->length = 5;
  export_array.length = 5;

  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_OK(&error,
                  AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                         "bulk_insert", &error));
  ADBCV_ASSERT_OK(&error,
                  AdbcStatementBind(&statement, &export_array, &export_schema, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementExecute(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSqlIngest", "read back data");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_OK(
      &error, AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_insert", &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementExecute(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementGetStream(&statement, &out, &error));

  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;

  NA_ASSERT_OK(out.get_schema(&out, &schema));
  ADBCV_ASSERT_EQ(1, schema.n_children);
  NA_ASSERT_OK(ArrowSchemaViewInit(&schema_view, schema.children[0], NULL));
  ADBCV_ASSERT_EQ(NANOARROW_TYPE_INT64, schema_view.data_type);

  struct ArrowArray array;
  NA_ASSERT_OK(out.get_next(&out, &array));
  ADBCV_ASSERT_NE(NULL, array.release);

  ADBCV_ASSERT_EQ(5, array.length);
  const int64_t* data = ((const int64_t*)array.children[0]->buffers[1]);
  ADBCV_ASSERT_TRUE(AdbcValidationIsSet(array.children[0], 0));
  ADBCV_ASSERT_TRUE(AdbcValidationIsSet(array.children[0], 1));
  ADBCV_ASSERT_FALSE(AdbcValidationIsSet(array.children[0], 2));
  ADBCV_ASSERT_FALSE(AdbcValidationIsSet(array.children[0], 3));
  ADBCV_ASSERT_TRUE(AdbcValidationIsSet(array.children[0], 4));
  ADBCV_ASSERT_EQ(16, data[0]);
  ADBCV_ASSERT_EQ(-1, data[1]);
  ADBCV_ASSERT_EQ(42, data[4]);

  array.release(&array);
  NA_ASSERT_OK(out.get_next(&out, &array));
  ADBCV_ASSERT_EQ(NULL, array.release);

  ADBCV_ASSERT_NE(NULL, schema.release);
  schema.release(&schema);
  ADBCV_ASSERT_NE(NULL, out.release);
  out.release(&out);
  ADBCV_ASSERT_OK(&error, AdbcStatementRelease(&statement, &error));

  AdbcValidateBeginCase(adbc_context, "StatementSqlIngest", "teardown");
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

  struct ArrowSchema schema;
  struct ArrowSchemaView schema_view;

  AdbcValidateBeginCase(adbc_context, "StatementSql", "prepare");
  ADBCV_ASSERT_OK(&error, AdbcStatementNew(&connection, &statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementPrepare(&statement, &error));

  ADBCV_ASSERT_OK(&error, AdbcStatementGetParameterSchema(&statement, &schema, &error));
  ADBCV_ASSERT_EQ(0, schema.n_children);
  schema.release(&schema);

  ADBCV_ASSERT_OK(&error, AdbcStatementExecute(&statement, &error));
  ADBCV_ASSERT_OK(&error, AdbcStatementGetStream(&statement, &out, &error));

  NA_ASSERT_OK(out.get_schema(&out, &schema));
  ADBCV_ASSERT_EQ(1, schema.n_children);
  NA_ASSERT_OK(ArrowSchemaViewInit(&schema_view, schema.children[0], NULL));
  ADBCV_ASSERT_EQ(NANOARROW_TYPE_INT64, schema_view.data_type);

  struct ArrowArray array;
  NA_ASSERT_OK(out.get_next(&out, &array));
  ADBCV_ASSERT_NE(NULL, array.release);

  ADBCV_ASSERT_TRUE(AdbcValidationIsSet(array.children[0], 0));
  ADBCV_ASSERT_EQ(42, ((int64_t*)array.children[0]->buffers[1])[0]);

  array.release(&array);
  NA_ASSERT_OK(out.get_next(&out, &array));
  ADBCV_ASSERT_EQ(NULL, array.release);

  ADBCV_ASSERT_NE(NULL, schema.release);
  schema.release(&schema);
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

  AdbcValidateBeginCase(adbc_context, "StatementSql", "teardown");
  ADBCV_ASSERT_OK(&error, AdbcConnectionRelease(&connection, &error));
  ADBCV_ASSERT_OK(&error, AdbcDatabaseRelease(&database, &error));
  if (error.release) error.release(&error);
}
