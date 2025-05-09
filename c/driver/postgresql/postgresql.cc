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

// A libpq-based PostgreSQL driver for ADBC.

#include <cstring>
#include <memory>

#include <arrow-adbc/adbc.h>

#include "connection.h"
#include "database.h"
#include "driver/common/utils.h"
#include "driver/framework/status.h"
#include "statement.h"

using adbc::driver::Status;
using adbcpq::PostgresConnection;
using adbcpq::PostgresDatabase;
using adbcpq::PostgresStatement;

// ---------------------------------------------------------------------
// ADBC interface implementation - as private functions so that these
// don't get replaced by the dynamic linker. If we implemented these
// under the Adbc* names, then in DriverInit, the linker may resolve
// functions to the address of the functions provided by the driver
// manager instead of our functions.
//
// We could also:
// - Play games with RTLD_DEEPBIND - but this doesn't work with ASan
// - Use __attribute__((visibility("protected"))) - but this is
//   apparently poorly supported by some linkers
// - Play with -Bsymbolic(-functions) - but this has other
//   consequences and complicates the build setup
//
// So in the end some manual effort here was chosen.

// ---------------------------------------------------------------------
// AdbcError

namespace {
const struct AdbcError* PostgresErrorFromArrayStream(struct ArrowArrayStream* stream,
                                                     AdbcStatusCode* status) {
  // Currently only valid for TupleReader
  return adbcpq::TupleReader::ErrorFromArrayStream(stream, status);
}

int PostgresErrorGetDetailCount(const struct AdbcError* error) {
  if (InternalAdbcIsCommonError(error)) {
    return InternalAdbcCommonErrorGetDetailCount(error);
  }

  if (error->vendor_code != ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
    return 0;
  }

  auto error_obj = reinterpret_cast<Status*>(error->private_data);
  return error_obj->CDetailCount();
}

struct AdbcErrorDetail PostgresErrorGetDetail(const struct AdbcError* error, int index) {
  if (InternalAdbcIsCommonError(error)) {
    return InternalAdbcCommonErrorGetDetail(error, index);
  }

  auto error_obj = reinterpret_cast<Status*>(error->private_data);
  return error_obj->CDetail(index);
}
}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
int AdbcErrorGetDetailCount(const struct AdbcError* error) {
  return PostgresErrorGetDetailCount(error);
}

struct AdbcErrorDetail AdbcErrorGetDetail(const struct AdbcError* error, int index) {
  return PostgresErrorGetDetail(error, index);
}

const struct AdbcError* AdbcErrorFromArrayStream(struct ArrowArrayStream* stream,
                                                 AdbcStatusCode* status) {
  return PostgresErrorFromArrayStream(stream, status);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

// ---------------------------------------------------------------------
// AdbcDatabase

namespace {
AdbcStatusCode PostgresDatabaseInit(struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode PostgresDatabaseNew(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  if (!database) {
    InternalAdbcSetError(error, "%s", "[libpq] database must not be null");
    return ADBC_STATUS_INVALID_STATE;
  }
  if (database->private_data) {
    InternalAdbcSetError(error, "%s", "[libpq] database is already initialized");
    return ADBC_STATUS_INVALID_STATE;
  }
  auto impl = std::make_shared<PostgresDatabase>();
  database->private_data = new std::shared_ptr<PostgresDatabase>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabaseRelease(struct AdbcDatabase* database,
                                       struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

AdbcStatusCode PostgresDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                         char* value, size_t* length,
                                         struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode PostgresDatabaseGetOptionBytes(struct AdbcDatabase* database,
                                              const char* key, uint8_t* value,
                                              size_t* length, struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode PostgresDatabaseGetOptionDouble(struct AdbcDatabase* database,
                                               const char* key, double* value,
                                               struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode PostgresDatabaseGetOptionInt(struct AdbcDatabase* database,
                                            const char* key, int64_t* value,
                                            struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode PostgresDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                         const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode PostgresDatabaseSetOptionBytes(struct AdbcDatabase* database,
                                              const char* key, const uint8_t* value,
                                              size_t length, struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode PostgresDatabaseSetOptionDouble(struct AdbcDatabase* database,
                                               const char* key, double value,
                                               struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode PostgresDatabaseSetOptionInt(struct AdbcDatabase* database,
                                            const char* key, int64_t value,
                                            struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}
}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
AdbcStatusCode AdbcDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                     char* value, size_t* length,
                                     struct AdbcError* error) {
  return PostgresDatabaseGetOption(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          uint8_t* value, size_t* length,
                                          struct AdbcError* error) {
  return PostgresDatabaseGetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t* value, struct AdbcError* error) {
  return PostgresDatabaseGetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double* value, struct AdbcError* error) {
  return PostgresDatabaseGetOptionDouble(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return PostgresDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return PostgresDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return PostgresDatabaseRelease(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return PostgresDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          const uint8_t* value, size_t length,
                                          struct AdbcError* error) {
  return PostgresDatabaseSetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t value, struct AdbcError* error) {
  return PostgresDatabaseSetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double value, struct AdbcError* error) {
  return PostgresDatabaseSetOptionDouble(database, key, value, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

// ---------------------------------------------------------------------
// AdbcConnection

namespace {
AdbcStatusCode PostgresConnectionCancel(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->Cancel(error);
}

AdbcStatusCode PostgresConnectionCommit(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->Commit(error);
}

AdbcStatusCode PostgresConnectionGetInfo(struct AdbcConnection* connection,
                                         const uint32_t* info_codes,
                                         size_t info_codes_length,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetInfo(connection, info_codes, info_codes_length, stream, error);
}

AdbcStatusCode PostgresConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetObjects(connection, depth, catalog, db_schema, table_name,
                            table_types, column_name, stream, error);
}

AdbcStatusCode PostgresConnectionGetOption(struct AdbcConnection* connection,
                                           const char* key, char* value, size_t* length,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode PostgresConnectionGetOptionBytes(struct AdbcConnection* connection,
                                                const char* key, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode PostgresConnectionGetOptionDouble(struct AdbcConnection* connection,
                                                 const char* key, double* value,
                                                 struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode PostgresConnectionGetOptionInt(struct AdbcConnection* connection,
                                              const char* key, int64_t* value,
                                              struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode PostgresConnectionGetStatistics(struct AdbcConnection* connection,
                                               const char* catalog, const char* db_schema,
                                               const char* table_name, char approximate,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetStatistics(catalog, db_schema, table_name, approximate == 1, out,
                               error);
}

AdbcStatusCode PostgresConnectionGetStatisticNames(struct AdbcConnection* connection,
                                                   struct ArrowArrayStream* out,
                                                   struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetStatisticNames(out, error);
}

AdbcStatusCode PostgresConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetTableSchema(catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode PostgresConnectionGetTableTypes(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* stream,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->GetTableTypes(connection, stream, error);
}

AdbcStatusCode PostgresConnectionInit(struct AdbcConnection* connection,
                                      struct AdbcDatabase* database,
                                      struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->Init(database, error);
}

AdbcStatusCode PostgresConnectionNew(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  auto impl = std::make_shared<PostgresConnection>();
  connection->private_data = new std::shared_ptr<PostgresConnection>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnectionReadPartition(struct AdbcConnection* connection,
                                               const uint8_t* serialized_partition,
                                               size_t serialized_length,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresConnectionRelease(struct AdbcConnection* connection,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

AdbcStatusCode PostgresConnectionRollback(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->Rollback(error);
}

AdbcStatusCode PostgresConnectionSetOption(struct AdbcConnection* connection,
                                           const char* key, const char* value,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode PostgresConnectionSetOptionBytes(struct AdbcConnection* connection,
                                                const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode PostgresConnectionSetOptionDouble(struct AdbcConnection* connection,
                                                 const char* key, double value,
                                                 struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode PostgresConnectionSetOptionInt(struct AdbcConnection* connection,
                                              const char* key, int64_t value,
                                              struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}

}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
AdbcStatusCode AdbcConnectionCancel(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return PostgresConnectionCancel(connection, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return PostgresConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     const uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* stream,
                                     struct AdbcError* error) {
  return PostgresConnectionGetInfo(connection, info_codes, info_codes_length, stream,
                                   error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name,
                                        struct ArrowArrayStream* stream,
                                        struct AdbcError* error) {
  return PostgresConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
                                      table_types, column_name, stream, error);
}

AdbcStatusCode AdbcConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  return PostgresConnectionGetOption(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  return PostgresConnectionGetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t* value,
                                          struct AdbcError* error) {
  return PostgresConnectionGetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  return PostgresConnectionGetOptionDouble(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetStatistics(struct AdbcConnection* connection,
                                           const char* catalog, const char* db_schema,
                                           const char* table_name, char approximate,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return PostgresConnectionGetStatistics(connection, catalog, db_schema, table_name,
                                         approximate, out, error);
}

AdbcStatusCode AdbcConnectionGetStatisticNames(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  return PostgresConnectionGetStatisticNames(connection, out, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  return PostgresConnectionGetTableSchema(connection, catalog, db_schema, table_name,
                                          schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return PostgresConnectionGetTableTypes(connection, stream, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return PostgresConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return PostgresConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return PostgresConnectionReadPartition(connection, serialized_partition,
                                         serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return PostgresConnectionRelease(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return PostgresConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return PostgresConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  return PostgresConnectionSetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionSetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t value,
                                          struct AdbcError* error) {
  return PostgresConnectionSetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  return PostgresConnectionSetOptionDouble(connection, key, value, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

// ---------------------------------------------------------------------
// AdbcStatement

namespace {
AdbcStatusCode PostgresStatementBind(struct AdbcStatement* statement,
                                     struct ArrowArray* values,
                                     struct ArrowSchema* schema,
                                     struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->Bind(values, schema, error);
}

AdbcStatusCode PostgresStatementBindStream(struct AdbcStatement* statement,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->Bind(stream, error);
}

AdbcStatusCode PostgresStatementCancel(struct AdbcStatement* statement,
                                       struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->Cancel(error);
}

AdbcStatusCode PostgresStatementExecutePartitions(struct AdbcStatement* statement,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcPartitions* partitions,
                                                  int64_t* rows_affected,
                                                  struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresStatementExecuteQuery(struct AdbcStatement* statement,
                                             struct ArrowArrayStream* output,
                                             int64_t* rows_affected,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->ExecuteQuery(output, rows_affected, error);
}

AdbcStatusCode PostgresStatementExecuteSchema(struct AdbcStatement* statement,
                                              struct ArrowSchema* schema,
                                              struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->ExecuteSchema(schema, error);
}

AdbcStatusCode PostgresStatementGetOption(struct AdbcStatement* statement,
                                          const char* key, char* value, size_t* length,
                                          struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode PostgresStatementGetOptionBytes(struct AdbcStatement* statement,
                                               const char* key, uint8_t* value,
                                               size_t* length, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode PostgresStatementGetOptionDouble(struct AdbcStatement* statement,
                                                const char* key, double* value,
                                                struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode PostgresStatementGetOptionInt(struct AdbcStatement* statement,
                                             const char* key, int64_t* value,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode PostgresStatementGetParameterSchema(struct AdbcStatement* statement,
                                                   struct ArrowSchema* schema,
                                                   struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->GetParameterSchema(schema, error);
}

AdbcStatusCode PostgresStatementNew(struct AdbcConnection* connection,
                                    struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  auto impl = std::make_shared<PostgresStatement>();
  statement->private_data = new std::shared_ptr<PostgresStatement>(impl);
  return impl->New(connection, error);
}

AdbcStatusCode PostgresStatementPrepare(struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->Prepare(error);
}

AdbcStatusCode PostgresStatementRelease(struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  auto status = (*ptr)->Release(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

AdbcStatusCode PostgresStatementSetOption(struct AdbcStatement* statement,
                                          const char* key, const char* value,
                                          struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode PostgresStatementSetOptionBytes(struct AdbcStatement* statement,
                                               const char* key, const uint8_t* value,
                                               size_t length, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode PostgresStatementSetOptionDouble(struct AdbcStatement* statement,
                                                const char* key, double value,
                                                struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode PostgresStatementSetOptionInt(struct AdbcStatement* statement,
                                             const char* key, int64_t value,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}

AdbcStatusCode PostgresStatementSetSqlQuery(struct AdbcStatement* statement,
                                            const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(query, error);
}
}  // namespace

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return PostgresStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return PostgresStatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementCancel(struct AdbcStatement* statement,
                                   struct AdbcError* error) {
  return PostgresStatementCancel(statement, error);
}

AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return PostgresStatementExecutePartitions(statement, schema, partitions, rows_affected,
                                            error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* output,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return PostgresStatementExecuteQuery(statement, output, rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteSchema(struct AdbcStatement* statement,
                                          ArrowSchema* schema, struct AdbcError* error) {
  return PostgresStatementExecuteSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementGetOption(struct AdbcStatement* statement, const char* key,
                                      char* value, size_t* length,
                                      struct AdbcError* error) {
  return PostgresStatementGetOption(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, uint8_t* value,
                                           size_t* length, struct AdbcError* error) {
  return PostgresStatementGetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t* value, struct AdbcError* error) {
  return PostgresStatementGetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double* value,
                                            struct AdbcError* error) {
  return PostgresStatementGetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  return PostgresStatementGetParameterSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return PostgresStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return PostgresStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return PostgresStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return PostgresStatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, const uint8_t* value,
                                           size_t length, struct AdbcError* error) {
  return PostgresStatementSetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementSetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t value, struct AdbcError* error) {
  return PostgresStatementSetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double value,
                                            struct AdbcError* error) {
  return PostgresStatementSetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return PostgresStatementSetSqlQuery(statement, query, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

extern "C" {
ADBC_EXPORT
AdbcStatusCode AdbcDriverPostgresqlInit(int version, void* raw_driver,
                                        struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0 && version != ADBC_VERSION_1_1_0) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  if (!raw_driver) return ADBC_STATUS_INVALID_ARGUMENT;

  auto* driver = reinterpret_cast<struct AdbcDriver*>(raw_driver);
  if (version >= ADBC_VERSION_1_1_0) {
    std::memset(driver, 0, ADBC_DRIVER_1_1_0_SIZE);

    driver->ErrorGetDetailCount = PostgresErrorGetDetailCount;
    driver->ErrorGetDetail = PostgresErrorGetDetail;
    driver->ErrorFromArrayStream = PostgresErrorFromArrayStream;

    driver->DatabaseGetOption = PostgresDatabaseGetOption;
    driver->DatabaseGetOptionBytes = PostgresDatabaseGetOptionBytes;
    driver->DatabaseGetOptionDouble = PostgresDatabaseGetOptionDouble;
    driver->DatabaseGetOptionInt = PostgresDatabaseGetOptionInt;
    driver->DatabaseSetOptionBytes = PostgresDatabaseSetOptionBytes;
    driver->DatabaseSetOptionDouble = PostgresDatabaseSetOptionDouble;
    driver->DatabaseSetOptionInt = PostgresDatabaseSetOptionInt;

    driver->ConnectionCancel = PostgresConnectionCancel;
    driver->ConnectionGetOption = PostgresConnectionGetOption;
    driver->ConnectionGetOptionBytes = PostgresConnectionGetOptionBytes;
    driver->ConnectionGetOptionDouble = PostgresConnectionGetOptionDouble;
    driver->ConnectionGetOptionInt = PostgresConnectionGetOptionInt;
    driver->ConnectionGetStatistics = PostgresConnectionGetStatistics;
    driver->ConnectionGetStatisticNames = PostgresConnectionGetStatisticNames;
    driver->ConnectionSetOptionBytes = PostgresConnectionSetOptionBytes;
    driver->ConnectionSetOptionDouble = PostgresConnectionSetOptionDouble;
    driver->ConnectionSetOptionInt = PostgresConnectionSetOptionInt;

    driver->StatementCancel = PostgresStatementCancel;
    driver->StatementExecuteSchema = PostgresStatementExecuteSchema;
    driver->StatementGetOption = PostgresStatementGetOption;
    driver->StatementGetOptionBytes = PostgresStatementGetOptionBytes;
    driver->StatementGetOptionDouble = PostgresStatementGetOptionDouble;
    driver->StatementGetOptionInt = PostgresStatementGetOptionInt;
    driver->StatementSetOptionBytes = PostgresStatementSetOptionBytes;
    driver->StatementSetOptionDouble = PostgresStatementSetOptionDouble;
    driver->StatementSetOptionInt = PostgresStatementSetOptionInt;
  } else {
    std::memset(driver, 0, ADBC_DRIVER_1_0_0_SIZE);
  }

  driver->DatabaseInit = PostgresDatabaseInit;
  driver->DatabaseNew = PostgresDatabaseNew;
  driver->DatabaseRelease = PostgresDatabaseRelease;
  driver->DatabaseSetOption = PostgresDatabaseSetOption;

  driver->ConnectionCommit = PostgresConnectionCommit;
  driver->ConnectionGetInfo = PostgresConnectionGetInfo;
  driver->ConnectionGetObjects = PostgresConnectionGetObjects;
  driver->ConnectionGetTableSchema = PostgresConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = PostgresConnectionGetTableTypes;
  driver->ConnectionInit = PostgresConnectionInit;
  driver->ConnectionNew = PostgresConnectionNew;
  driver->ConnectionReadPartition = PostgresConnectionReadPartition;
  driver->ConnectionRelease = PostgresConnectionRelease;
  driver->ConnectionRollback = PostgresConnectionRollback;
  driver->ConnectionSetOption = PostgresConnectionSetOption;

  driver->StatementBind = PostgresStatementBind;
  driver->StatementBindStream = PostgresStatementBindStream;
  driver->StatementExecutePartitions = PostgresStatementExecutePartitions;
  driver->StatementExecuteQuery = PostgresStatementExecuteQuery;
  driver->StatementGetParameterSchema = PostgresStatementGetParameterSchema;
  driver->StatementNew = PostgresStatementNew;
  driver->StatementPrepare = PostgresStatementPrepare;
  driver->StatementRelease = PostgresStatementRelease;
  driver->StatementSetOption = PostgresStatementSetOption;
  driver->StatementSetSqlQuery = PostgresStatementSetSqlQuery;

  return ADBC_STATUS_OK;
}

#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
ADBC_EXPORT
AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  return AdbcDriverPostgresqlInit(version, raw_driver, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS
}
