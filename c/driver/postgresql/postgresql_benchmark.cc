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

#include <string>

#include <benchmark/benchmark.h>
#include <nanoarrow/nanoarrow.hpp>

#include "arrow-adbc/adbc.h"
#include "validation/adbc_validation_util.h"

#define _ADBC_BENCHMARK_RETURN_NOT_OK_IMPL(NAME, EXPR) \
  do {                                                 \
    const int NAME = (EXPR);                           \
    if (NAME) {                                        \
      state.SkipWithError(error.message);              \
      error.release(&error);                           \
      return;                                          \
    }                                                  \
  } while (0)

#define ADBC_BENCHMARK_RETURN_NOT_OK(EXPR)                                             \
  _ADBC_BENCHMARK_RETURN_NOT_OK_IMPL(_NANOARROW_MAKE_NAME(errno_status_, __COUNTER__), \
                                     EXPR)

static void BM_PostgresqlExecute(benchmark::State& state) {
  const char* uri = std::getenv("ADBC_POSTGRESQL_TEST_URI");
  if (!uri || !strcmp(uri, "")) {
    state.SkipWithError("ADBC_POSTGRESQL_TEST_URI not set!");
    return;
  }
  adbc_validation::Handle<struct AdbcDatabase> database;
  struct AdbcError error;

  ADBC_BENCHMARK_RETURN_NOT_OK(AdbcDatabaseNew(&database.value, &error));
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcDatabaseSetOption(&database.value, "uri", uri, &error));
  ADBC_BENCHMARK_RETURN_NOT_OK(AdbcDatabaseInit(&database.value, &error));

  adbc_validation::Handle<struct AdbcConnection> connection;
  ADBC_BENCHMARK_RETURN_NOT_OK(AdbcConnectionNew(&connection.value, &error));
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcConnectionInit(&connection.value, &database.value, &error));

  adbc_validation::Handle<struct AdbcStatement> statement;
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementNew(&connection.value, &statement.value, &error));

  const char* drop_query = "DROP TABLE IF EXISTS adbc_postgresql_ingest_benchmark";
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetSqlQuery(&statement.value, drop_query, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error));

  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ADBC_BENCHMARK_RETURN_NOT_OK(
      adbc_validation::MakeSchema(&schema.value, {
                                                     {"bools", NANOARROW_TYPE_BOOL},
                                                     {"int16s", NANOARROW_TYPE_INT16},
                                                     {"int32s", NANOARROW_TYPE_INT32},
                                                     {"int64s", NANOARROW_TYPE_INT64},
                                                     {"floats", NANOARROW_TYPE_FLOAT},
                                                     {"doubles", NANOARROW_TYPE_DOUBLE},
                                                 }));

  if (ArrowArrayInitFromSchema(&array.value, &schema.value, &na_error) != NANOARROW_OK) {
    state.SkipWithError("Call to ArrowArrayInitFromSchema failed!");
    error.release(&error);
    return;
  }

  if (ArrowArrayStartAppending(&array.value) != NANOARROW_OK) {
    state.SkipWithError("Call to ArrowArrayStartAppending failed!");
    error.release(&error);
    return;
  }

  const size_t n_zeros = 1000;
  const size_t n_ones = 1000;

  for (size_t i = 0; i < n_zeros; i++) {
    // assumes fixed size primitive layouts for now
    ArrowBufferAppendInt8(ArrowArrayBuffer(array.value.children[0], 1), 0);
    ArrowBufferAppendInt16(ArrowArrayBuffer(array.value.children[1], 1), 0);
    ArrowBufferAppendInt32(ArrowArrayBuffer(array.value.children[2], 1), 0);
    ArrowBufferAppendInt64(ArrowArrayBuffer(array.value.children[3], 1), 0);
    ArrowBufferAppendFloat(ArrowArrayBuffer(array.value.children[4], 1), 0.0);
    ArrowBufferAppendDouble(ArrowArrayBuffer(array.value.children[5], 1), 0.0);
  }
  for (size_t i = 0; i < n_ones; i++) {
    // assumes fixed size primitive layouts for now
    ArrowBufferAppendInt8(ArrowArrayBuffer(array.value.children[0], 1), 1);
    ArrowBufferAppendInt16(ArrowArrayBuffer(array.value.children[1], 1), 1);
    ArrowBufferAppendInt32(ArrowArrayBuffer(array.value.children[2], 1), 1);
    ArrowBufferAppendInt64(ArrowArrayBuffer(array.value.children[3], 1), 1);
    ArrowBufferAppendFloat(ArrowArrayBuffer(array.value.children[4], 1), 1.0);
    ArrowBufferAppendDouble(ArrowArrayBuffer(array.value.children[5], 1), 1.0);
  }

  for (int64_t i = 0; i < array.value.n_children; i++) {
    array.value.children[i]->length = n_zeros + n_ones;
  }
  array.value.length = n_zeros + n_ones;

  if (ArrowArrayFinishBuildingDefault(&array.value, &na_error) != NANOARROW_OK) {
    state.SkipWithError("Call to ArrowArrayFinishBuildingDefault failed");
    error.release(&error);
    return;
  }

  const char* create_query =
      "CREATE TABLE adbc_postgresql_ingest_benchmark (bools BOOLEAN, int16s SMALLINT, "
      "int32s INTEGER, int64s BIGINT, floats REAL, doubles DOUBLE PRECISION)";

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetSqlQuery(&statement.value, create_query, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error));

  adbc_validation::Handle<struct AdbcStatement> insert_stmt;
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementNew(&connection.value, &insert_stmt.value, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetOption(&insert_stmt.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                             "adbc_postgresql_ingest_benchmark", &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetOption(&insert_stmt.value, ADBC_INGEST_OPTION_MODE,
                             ADBC_INGEST_OPTION_MODE_APPEND, &error));

  for (auto _ : state) {
    AdbcStatementBind(&insert_stmt.value, &array.value, &schema.value, &error);
    AdbcStatementExecuteQuery(&insert_stmt.value, nullptr, nullptr, &error);
  }

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetSqlQuery(&statement.value, drop_query, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error));
}

static void BM_PostgresqlDecimalWrite(benchmark::State& state) {
  const char* uri = std::getenv("ADBC_POSTGRESQL_TEST_URI");
  if (!uri || !strcmp(uri, "")) {
    state.SkipWithError("ADBC_POSTGRESQL_TEST_URI not set!");
    return;
  }
  adbc_validation::Handle<struct AdbcDatabase> database;
  struct AdbcError error;

  ADBC_BENCHMARK_RETURN_NOT_OK(AdbcDatabaseNew(&database.value, &error));
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcDatabaseSetOption(&database.value, "uri", uri, &error));
  ADBC_BENCHMARK_RETURN_NOT_OK(AdbcDatabaseInit(&database.value, &error));

  adbc_validation::Handle<struct AdbcConnection> connection;
  ADBC_BENCHMARK_RETURN_NOT_OK(AdbcConnectionNew(&connection.value, &error));
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcConnectionInit(&connection.value, &database.value, &error));

  adbc_validation::Handle<struct AdbcStatement> statement;
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementNew(&connection.value, &statement.value, &error));

  const char* drop_query = "DROP TABLE IF EXISTS adbc_postgresql_ingest_benchmark";
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetSqlQuery(&statement.value, drop_query, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error));

  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  constexpr enum ArrowType type = NANOARROW_TYPE_DECIMAL128;
  constexpr int32_t bitwidth = 128;
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 8;
  constexpr size_t ncols = 5;
  ArrowSchemaInit(&schema.value);
  if (ArrowSchemaSetTypeStruct(&schema.value, ncols) != NANOARROW_OK) {
    state.SkipWithError("Call to ArrowSchemaSetTypeStruct failed!");
    error.release(&error);
    return;
  }

  for (size_t i = 0; i < ncols; i++) {
    if (PrivateArrowSchemaSetTypeDecimal(schema.value.children[i], type, precision,
                                         scale) != NANOARROW_OK) {
      state.SkipWithError("Call to ArrowSchemaSetTypeDecimal failed!");
      error.release(&error);
      return;
    }

    std::string colname = "col" + std::to_string(i);
    if (ArrowSchemaSetName(schema.value.children[i], colname.c_str()) != NANOARROW_OK) {
      state.SkipWithError("Call to ArrowSchemaSetName failed!");
      error.release(&error);
      return;
    }
  }
  if (ArrowArrayInitFromSchema(&array.value, &schema.value, &na_error) != NANOARROW_OK) {
    state.SkipWithError("Call to ArrowArrayInitFromSchema failed!");
    error.release(&error);
    return;
  }

  if (ArrowArrayStartAppending(&array.value) != NANOARROW_OK) {
    state.SkipWithError("Call to ArrowArrayStartAppending failed!");
    error.release(&error);
    return;
  }

  constexpr size_t nrows = 1000;
  struct ArrowDecimal decimal;
  ArrowDecimalInit(&decimal, bitwidth, precision, scale);
  for (size_t i = 0; i < nrows; i++) {
    for (size_t j = 0; j < ncols; j++) {
      ArrowDecimalSetInt(&decimal, i + j);
      if (ArrowArrayAppendDecimal(array.value.children[j], &decimal) != NANOARROW_OK) {
        state.SkipWithError("Call to ArrowArrayAppendDecimal failed");
        error.release(&error);
        return;
      }
    }
  }

  for (int64_t i = 0; i < array.value.n_children; i++) {
    array.value.children[i]->length = nrows;
  }
  array.value.length = nrows;

  if (ArrowArrayFinishBuildingDefault(&array.value, &na_error) != NANOARROW_OK) {
    state.SkipWithError("Call to ArrowArrayFinishBuildingDefault failed");
    error.release(&error);
    return;
  }

  const char* create_query =
      "CREATE TABLE adbc_postgresql_ingest_benchmark (col0 DECIMAL(38, 8), "
      "col1 DECIMAL(38, 8), col2 DECIMAL(38, 8), col3 DECIMAL(38, 8), col4 DECIMAL(38, "
      "8))";

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetSqlQuery(&statement.value, create_query, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error));

  adbc_validation::Handle<struct AdbcStatement> insert_stmt;
  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementNew(&connection.value, &insert_stmt.value, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetOption(&insert_stmt.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                             "adbc_postgresql_ingest_benchmark", &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetOption(&insert_stmt.value, ADBC_INGEST_OPTION_MODE,
                             ADBC_INGEST_OPTION_MODE_APPEND, &error));

  for (auto _ : state) {
    AdbcStatementBind(&insert_stmt.value, &array.value, &schema.value, &error);
    AdbcStatementExecuteQuery(&insert_stmt.value, nullptr, nullptr, &error);
  }

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementSetSqlQuery(&statement.value, drop_query, &error));

  ADBC_BENCHMARK_RETURN_NOT_OK(
      AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error));
}

// TODO: we are limited to only 1 iteration as AdbcStatementBind is part of
// the benchmark loop, but releases the array when it is done
BENCHMARK(BM_PostgresqlExecute)->Iterations(1);
BENCHMARK(BM_PostgresqlDecimalWrite)->Iterations(1);
BENCHMARK_MAIN();
