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

// Utilities for executing SQLite statements.

#pragma once

#include <arrow-adbc/adbc.h>
#include <nanoarrow/nanoarrow.h>
#include <sqlite3.h>

#ifdef __cplusplus
extern "C" {
#endif

/// \brief Helper to manage binding data to a SQLite statement.
struct ADBC_EXPORT AdbcSqliteBinder {
  // State
  struct ArrowSchema schema;
  struct ArrowArrayStream params;
  enum ArrowType* types;

  // Scratch space
  struct ArrowArray array;
  struct ArrowArrayView batch;
  int64_t next_row;
};

ADBC_EXPORT
AdbcStatusCode AdbcSqliteBinderSetArrayStream(struct AdbcSqliteBinder* binder,
                                              struct ArrowArrayStream* values,
                                              struct AdbcError* error);
ADBC_EXPORT
AdbcStatusCode AdbcSqliteBinderBindNext(struct AdbcSqliteBinder* binder, sqlite3* conn,
                                        sqlite3_stmt* stmt, char* finished,
                                        struct AdbcError* error);
ADBC_EXPORT
void AdbcSqliteBinderRelease(struct AdbcSqliteBinder* binder);

/// \brief Initialize an ArrowArrayStream from a sqlite3_stmt.
/// \param[in] db The SQLite connection.
/// \param[in] stmt The SQLite statement.
/// \param[in] binder Query parameters to bind, if provided.
/// \param[in] infer_rows How many rows to read to infer the Arrow schema.
/// \param[out] stream The stream to export to.
/// \param[out] error Error details, if needed.
ADBC_EXPORT
AdbcStatusCode AdbcSqliteExportReader(sqlite3* db, sqlite3_stmt* stmt,
                                      struct AdbcSqliteBinder* binder, size_t batch_size,
                                      struct ArrowArrayStream* stream,
                                      struct AdbcError* error);

#ifdef __cplusplus
}
#endif
