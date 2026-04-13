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

// An ADBC driver for IBM DB2, using DB2 CLI / ODBC.

#include <arrow-adbc/adbc.h>

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/base_driver.h"

#include "connection.h"
#include "database.h"
#include "statement.h"

namespace adbc::db2 {

using Db2Driver = driver::Driver<Db2Database, Db2Connection, Db2Statement>;

}  // namespace adbc::db2

// ---- ADBC common entry points (when built as standalone driver) ----

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database,
                                struct AdbcError* error) {
  return adbc::db2::Db2Driver::CDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database,
                               struct AdbcError* error) {
  return adbc::db2::Db2Driver::CNew<>(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return adbc::db2::Db2Driver::CRelease<>(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOption<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                     char* value, size_t* length,
                                     struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOption<>(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionInt<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionDouble<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t* value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionInt<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double* value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionDouble<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          uint8_t* value, size_t* length,
                                          struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionBytes<>(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseSetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          const uint8_t* value, size_t length,
                                          struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionBytes<>(database, key, value, length, error);
}

// Connection

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     const uint32_t* info_codes,
                                     size_t info_codes_length,
                                     struct ArrowArrayStream* stream,
                                     struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionGetInfo(connection, info_codes,
                                                   info_codes_length, stream, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name,
                                        struct ArrowArrayStream* stream,
                                        struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionGetObjects(
      connection, depth, catalog, db_schema, table_name, table_types, column_name,
      stream, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionGetTableSchema(connection, catalog, db_schema,
                                                          table_name, schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionGetTableTypes(connection, stream, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return adbc::db2::Db2Driver::CNew<>(connection, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return adbc::db2::Db2Driver::CRelease<>(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOption<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOption<>(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionSetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t value,
                                          struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionInt<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionDouble<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t* value,
                                          struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionInt<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionDouble<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionBytes<>(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionSetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionBytes<>(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionReadPartition(
      connection, serialized_partition, serialized_length, out, error);
}

// Statement

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* output,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementExecuteQuery(statement, output, rows_affected,
                                                       error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return adbc::db2::Db2Driver::CRelease<>(statement, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOption<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOption(struct AdbcStatement* statement, const char* key,
                                      char* value, size_t* length,
                                      struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOption<>(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementSetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionInt<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double value,
                                            struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionDouble<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t* value, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionInt<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double* value,
                                            struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionDouble<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, uint8_t* value,
                                           size_t* length, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CGetOptionBytes<>(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementSetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, const uint8_t* value,
                                           size_t length, struct AdbcError* error) {
  return adbc::db2::Db2Driver::CSetOptionBytes<>(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementGetParameterSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              struct ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementExecutePartitions(
      statement, schema, partitions, rows_affected, error);
}

AdbcStatusCode AdbcConnectionCancel(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionCancel(connection, error);
}

AdbcStatusCode AdbcConnectionGetStatisticNames(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  return adbc::db2::Db2Driver::CConnectionGetStatisticNames(connection, out, error);
}

AdbcStatusCode AdbcStatementCancel(struct AdbcStatement* statement,
                                   struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementCancel(statement, error);
}

AdbcStatusCode AdbcStatementExecuteSchema(struct AdbcStatement* statement,
                                          struct ArrowSchema* schema,
                                          struct AdbcError* error) {
  return adbc::db2::Db2Driver::CStatementExecuteSchema(statement, schema, error);
}

[[maybe_unused]] ADBC_EXPORT AdbcStatusCode AdbcDriverInit(int version,
                                                           void* raw_driver,
                                                           AdbcError* error) {
  return adbc::db2::Db2Driver::Init(version, raw_driver, error);
}

#endif  // ADBC_NO_COMMON_ENTRYPOINTS

extern "C" {

[[maybe_unused]] ADBC_EXPORT AdbcStatusCode AdbcDriverDb2Init(int version,
                                                              void* raw_driver,
                                                              AdbcError* error) {
  return adbc::db2::Db2Driver::Init(version, raw_driver, error);
}

}  // extern "C"
