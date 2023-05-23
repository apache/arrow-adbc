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

#include <adbc.h>

#include "connection.h"
#include "database.h"
#include "statement.h"
#include "utils.h"

using adbcpq::PostgresConnection;
using adbcpq::PostgresDatabase;
using adbcpq::PostgresStatement;

// ---------------------------------------------------------------------
// ADBC interface implementation - as private functions so that these
// don't get replaced by the dynamic linker. If we implemented these
// under the Adbc* names, then DriverInit, the linker may resolve
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
    SetError(error, "%s", "[libpq] database must not be null");
    return ADBC_STATUS_INVALID_STATE;
  }
  if (database->private_data) {
    SetError(error, "%s", "[libpq] database is already initialized");
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

AdbcStatusCode PostgresDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                         const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}
}  // namespace

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

// ---------------------------------------------------------------------
// AdbcConnection

namespace {
AdbcStatusCode PostgresConnectionCommit(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  return (*ptr)->Commit(error);
}

AdbcStatusCode PostgresConnectionGetInfo(struct AdbcConnection* connection,
                                         uint32_t* info_codes, size_t info_codes_length,
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
  return ADBC_STATUS_NOT_IMPLEMENTED;
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

}  // namespace
AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return PostgresConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     uint32_t* info_codes, size_t info_codes_length,
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

AdbcStatusCode PostgresStatementGetPartitionDesc(struct AdbcStatement* statement,
                                                 uint8_t* partition_desc,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                     size_t* length,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
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

AdbcStatusCode PostgresStatementSetSqlQuery(struct AdbcStatement* statement,
                                            const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<PostgresStatement>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(query, error);
}
}  // namespace

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

AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             uint8_t* partition_desc,
                                             struct AdbcError* error) {
  return PostgresStatementGetPartitionDesc(statement, partition_desc, error);
}

AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  return PostgresStatementGetPartitionDescSize(statement, length, error);
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

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return PostgresStatementSetSqlQuery(statement, query, error);
}

extern "C" {
ADBC_EXPORT
AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  if (!raw_driver) return ADBC_STATUS_INVALID_ARGUMENT;

  auto* driver = reinterpret_cast<struct AdbcDriver*>(raw_driver);
  std::memset(driver, 0, ADBC_DRIVER_1_0_0_SIZE);
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
}
