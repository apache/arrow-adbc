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


#include <benchmark/benchmark.h>
#include <nanoarrow/nanoarrow.hpp>

#include "adbc.h"
#include "validation/adbc_validation_util.h"


static void BM_PostgresqlExecute(benchmark::State& state) {
  const char* uri = std::getenv("ADBC_POSTGRESQL_TEST_URI");
  if (!uri || !strcmp(uri, "")) {
    state.SkipWithError("ADBC_POSTGRESQL_TEST_URI not set!");
    return;
  }
  adbc_validation::Handle<struct AdbcDatabase> database;
  struct AdbcError error;

  if (AdbcDatabaseNew(&database.value, &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcDatabaseSetOption(&database.value, "uri", uri, &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcDatabaseInit(&database.value, &error) != ADBC_STATUS_OK) {
state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  adbc_validation::Handle<struct AdbcConnection> connection;
  if (AdbcConnectionNew(&connection.value, &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcConnectionInit(&connection.value, &database.value, &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  adbc_validation::Handle<struct AdbcStatement> statement;
  if (AdbcStatementNew(&connection.value, &statement.value, &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  const char* drop_query = "DROP TABLE IF EXISTS adbc_postgresql_ingest_benchmark";
  if (AdbcStatementSetSqlQuery(&statement.value, drop_query, &error)
      != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error)
      != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  if (adbc_validation::MakeSchema(&schema.value, {
        {"bools", NANOARROW_TYPE_BOOL},
        {"int16s", NANOARROW_TYPE_INT16},
        {"int32s", NANOARROW_TYPE_INT32},
        {"int64s", NANOARROW_TYPE_INT64},
        {"floats", NANOARROW_TYPE_FLOAT},
        {"doubles", NANOARROW_TYPE_DOUBLE},
      }) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (ArrowArrayInitFromSchema(&array.value, &schema.value, &na_error) != NANOARROW_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (ArrowArrayStartAppending(&array.value) != NANOARROW_OK) {
    state.SkipWithError(error.message);
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
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  const char* create_query =
    "CREATE TABLE adbc_postgresql_ingest_benchmark (bools BOOLEAN, int16s SMALLINT, "
    "int32s INTEGER, int64s BIGINT, floats REAL, doubles DOUBLE PRECISION)";

  if (AdbcStatementSetSqlQuery(&statement.value, create_query, &error)
      != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error)
      != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  adbc_validation::Handle<struct AdbcStatement> insert_stmt;
  if (AdbcStatementNew(&connection.value, &insert_stmt.value, &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcStatementSetOption(&insert_stmt.value,
                             ADBC_INGEST_OPTION_TARGET_TABLE,
                             "adbc_postgresql_ingest_benchmark",
                             &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcStatementSetOption(&insert_stmt.value,
                             ADBC_INGEST_OPTION_MODE,
                             ADBC_INGEST_OPTION_MODE_APPEND,
                             &error) != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  for (auto _ : state) {
    AdbcStatementBind(&insert_stmt.value, &array.value, &schema.value, &error);
    AdbcStatementExecuteQuery(&insert_stmt.value, nullptr, nullptr, &error);
  }

  if (AdbcStatementSetSqlQuery(&statement.value, drop_query, &error)
      != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }

  if (AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error)
      != ADBC_STATUS_OK) {
    state.SkipWithError(error.message);
    error.release(&error);
    return;
  }
}

BENCHMARK(BM_PostgresqlExecute)->Iterations(1);
BENCHMARK_MAIN();
