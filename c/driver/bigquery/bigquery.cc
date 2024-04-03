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

// A google-cloud-sdk based BigQuery driver for ADBC.

#include <cstring>
#include <memory>

#include <adbc.h>
#include <absl/log/initialize.h>

#include "common/utils.h"
#include "connection.h"
#include "database.h"
#include "statement.h"

using adbc_bigquery::BigqueryConnection;
using adbc_bigquery::BigqueryDatabase;
using adbc_bigquery::BigqueryStatement;

// AdbcError

namespace {
const struct AdbcError* BigqueryErrorFromArrayStream(struct ArrowArrayStream* stream,
                                                     AdbcStatusCode* status) {
  // TODO_BIGQUERY: Implement this function.
  return nullptr;
}
}  // namespace

int AdbcErrorGetDetailCount(const struct AdbcError* error) {
  return CommonErrorGetDetailCount(error);
}

struct AdbcErrorDetail AdbcErrorGetDetail(const struct AdbcError* error, int index) {
  return CommonErrorGetDetail(error, index);
}

const struct AdbcError* AdbcErrorFromArrayStream(struct ArrowArrayStream* stream,
                                                 AdbcStatusCode* status) {
  return BigqueryErrorFromArrayStream(stream, status);
}

// ---------------------------------------------------------------------
// AdbcDatabase

namespace {
AdbcStatusCode BigqueryDatabaseInit(struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode BigqueryDatabaseNew(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  if (!database) {
    SetError(error, "%s", "[bigquery] database must not be null");
    return ADBC_STATUS_INVALID_STATE;
  }
  if (database->private_data) {
    SetError(error, "%s", "[bigquery] database is already initialized");
    return ADBC_STATUS_INVALID_STATE;
  }
  auto impl = std::make_shared<BigqueryDatabase>();
  database->private_data = new std::shared_ptr<BigqueryDatabase>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryDatabaseRelease(struct AdbcDatabase* database,
                                       struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

AdbcStatusCode BigqueryDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                         char* value, size_t* length,
                                         struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode BigqueryDatabaseGetOptionBytes(struct AdbcDatabase* database,
                                              const char* key, uint8_t* value,
                                              size_t* length, struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode BigqueryDatabaseGetOptionDouble(struct AdbcDatabase* database,
                                               const char* key, double* value,
                                               struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode BigqueryDatabaseGetOptionInt(struct AdbcDatabase* database,
                                            const char* key, int64_t* value,
                                            struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode BigqueryDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                         const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode BigqueryDatabaseSetOptionBytes(struct AdbcDatabase* database,
                                              const char* key, const uint8_t* value,
                                              size_t length, struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode BigqueryDatabaseSetOptionDouble(struct AdbcDatabase* database,
                                               const char* key, double value,
                                               struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode BigqueryDatabaseSetOptionInt(struct AdbcDatabase* database,
                                            const char* key, int64_t value,
                                            struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr = reinterpret_cast<std::shared_ptr<BigqueryDatabase>*>(database->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}
}  // namespace

AdbcStatusCode AdbcDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                     char* value, size_t* length,
                                     struct AdbcError* error) {
  return BigqueryDatabaseGetOption(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          uint8_t* value, size_t* length,
                                          struct AdbcError* error) {
  return BigqueryDatabaseGetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t* value, struct AdbcError* error) {
  return BigqueryDatabaseGetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double* value, struct AdbcError* error) {
  return BigqueryDatabaseGetOptionDouble(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return BigqueryDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return BigqueryDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return BigqueryDatabaseRelease(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return BigqueryDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          const uint8_t* value, size_t length,
                                          struct AdbcError* error) {
  return BigqueryDatabaseSetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t value, struct AdbcError* error) {
  return BigqueryDatabaseSetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double value, struct AdbcError* error) {
  return BigqueryDatabaseSetOptionDouble(database, key, value, error);
}

// ---------------------------------------------------------------------
// AdbcConnection

namespace {
AdbcStatusCode BigqueryConnectionCancel(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->Cancel(error);
}

AdbcStatusCode BigqueryConnectionCommit(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->Commit(error);
}

AdbcStatusCode BigqueryConnectionGetInfo(struct AdbcConnection* connection,
                                         const uint32_t* info_codes,
                                         size_t info_codes_length,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetInfo(connection, info_codes, info_codes_length, stream, error);
}

AdbcStatusCode BigqueryConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetObjects(connection, depth, catalog, db_schema, table_name,
                            table_types, column_name, stream, error);
}

AdbcStatusCode BigqueryConnectionGetOption(struct AdbcConnection* connection,
                                           const char* key, char* value, size_t* length,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode BigqueryConnectionGetOptionBytes(struct AdbcConnection* connection,
                                                const char* key, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode BigqueryConnectionGetOptionDouble(struct AdbcConnection* connection,
                                                 const char* key, double* value,
                                                 struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode BigqueryConnectionGetOptionInt(struct AdbcConnection* connection,
                                              const char* key, int64_t* value,
                                              struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode BigqueryConnectionGetStatistics(struct AdbcConnection* connection,
                                               const char* catalog, const char* db_schema,
                                               const char* table_name, char approximate,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetStatistics(catalog, db_schema, table_name, approximate == 1, out,
                               error);
}

AdbcStatusCode BigqueryConnectionGetStatisticNames(struct AdbcConnection* connection,
                                                   struct ArrowArrayStream* out,
                                                   struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetStatisticNames(out, error);
}

AdbcStatusCode BigqueryConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetTableSchema(catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode BigqueryConnectionGetTableTypes(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* stream,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->GetTableTypes(connection, stream, error);
}

AdbcStatusCode BigqueryConnectionInit(struct AdbcConnection* connection,
                                      struct AdbcDatabase* database,
                                      struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->Init(database, error);
}

AdbcStatusCode BigqueryConnectionNew(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  auto impl = std::make_shared<BigqueryConnection>();
  connection->private_data = new std::shared_ptr<BigqueryConnection>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode BigqueryConnectionReadPartition(struct AdbcConnection* connection,
                                               const uint8_t* serialized_partition,
                                               size_t serialized_length,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryConnectionRelease(struct AdbcConnection* connection,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

AdbcStatusCode BigqueryConnectionRollback(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->Rollback(error);
}

AdbcStatusCode BigqueryConnectionSetOption(struct AdbcConnection* connection,
                                           const char* key, const char* value,
                                           struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode BigqueryConnectionSetOptionBytes(struct AdbcConnection* connection,
                                                const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode BigqueryConnectionSetOptionDouble(struct AdbcConnection* connection,
                                                 const char* key, double value,
                                                 struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode BigqueryConnectionSetOptionInt(struct AdbcConnection* connection,
                                              const char* key, int64_t value,
                                              struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryConnection>*>(connection->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}

}  // namespace

AdbcStatusCode AdbcConnectionCancel(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return BigqueryConnectionCancel(connection, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return BigqueryConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     const uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* stream,
                                     struct AdbcError* error) {
  return BigqueryConnectionGetInfo(connection, info_codes, info_codes_length, stream,
                                   error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name,
                                        struct ArrowArrayStream* stream,
                                        struct AdbcError* error) {
  return BigqueryConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
                                      table_types, column_name, stream, error);
}

AdbcStatusCode AdbcConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  return BigqueryConnectionGetOption(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  return BigqueryConnectionGetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t* value,
                                          struct AdbcError* error) {
  return BigqueryConnectionGetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  return BigqueryConnectionGetOptionDouble(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetStatistics(struct AdbcConnection* connection,
                                           const char* catalog, const char* db_schema,
                                           const char* table_name, char approximate,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return BigqueryConnectionGetStatistics(connection, catalog, db_schema, table_name,
                                         approximate, out, error);
}

AdbcStatusCode AdbcConnectionGetStatisticNames(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  return BigqueryConnectionGetStatisticNames(connection, out, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  return BigqueryConnectionGetTableSchema(connection, catalog, db_schema, table_name,
                                          schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return BigqueryConnectionGetTableTypes(connection, stream, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return BigqueryConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return BigqueryConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return BigqueryConnectionReadPartition(connection, serialized_partition,
                                         serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return BigqueryConnectionRelease(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return BigqueryConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return BigqueryConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  return BigqueryConnectionSetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionSetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t value,
                                          struct AdbcError* error) {
  return BigqueryConnectionSetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  return BigqueryConnectionSetOptionDouble(connection, key, value, error);
}

// ---------------------------------------------------------------------

// AdbcStatement

namespace {
AdbcStatusCode BigqueryStatementBind(struct AdbcStatement* statement,
                                     struct ArrowArray* values,
                                     struct ArrowSchema* schema,
                                     struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->Bind(values, schema, error);
}

AdbcStatusCode BigqueryStatementBindStream(struct AdbcStatement* statement,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->Bind(stream, error);
}

AdbcStatusCode BigqueryStatementCancel(struct AdbcStatement* statement,
                                       struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->Cancel(error);
}

AdbcStatusCode BigqueryStatementExecutePartitions(struct AdbcStatement* statement,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcPartitions* partitions,
                                                  int64_t* rows_affected,
                                                  struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode BigqueryStatementExecuteQuery(struct AdbcStatement* statement,
                                             struct ArrowArrayStream* output,
                                             int64_t* rows_affected,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->ExecuteQuery(output, rows_affected, error);
}

AdbcStatusCode BigqueryStatementExecuteSchema(struct AdbcStatement* statement,
                                              struct ArrowSchema* schema,
                                              struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->ExecuteSchema(schema, error);
}

AdbcStatusCode BigqueryStatementGetOption(struct AdbcStatement* statement,
                                          const char* key, char* value, size_t* length,
                                          struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->GetOption(key, value, length, error);
}

AdbcStatusCode BigqueryStatementGetOptionBytes(struct AdbcStatement* statement,
                                               const char* key, uint8_t* value,
                                               size_t* length, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->GetOptionBytes(key, value, length, error);
}

AdbcStatusCode BigqueryStatementGetOptionDouble(struct AdbcStatement* statement,
                                                const char* key, double* value,
                                                struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->GetOptionDouble(key, value, error);
}

AdbcStatusCode BigqueryStatementGetOptionInt(struct AdbcStatement* statement,
                                             const char* key, int64_t* value,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->GetOptionInt(key, value, error);
}

AdbcStatusCode BigqueryStatementGetParameterSchema(struct AdbcStatement* statement,
                                                   struct ArrowSchema* schema,
                                                   struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->GetParameterSchema(schema, error);
}

AdbcStatusCode BigqueryStatementNew(struct AdbcConnection* connection,
                                    struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  auto impl = std::make_shared<BigqueryStatement>();
  statement->private_data = new std::shared_ptr<BigqueryStatement>(impl);
  return impl->New(connection, error);
}

AdbcStatusCode BigqueryStatementPrepare(struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->Prepare(error);
}

AdbcStatusCode BigqueryStatementRelease(struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  auto status = (*ptr)->Release(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

AdbcStatusCode BigqueryStatementSetOption(struct AdbcStatement* statement,
                                          const char* key, const char* value,
                                          struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode BigqueryStatementSetOptionBytes(struct AdbcStatement* statement,
                                               const char* key, const uint8_t* value,
                                               size_t length, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->SetOptionBytes(key, value, length, error);
}

AdbcStatusCode BigqueryStatementSetOptionDouble(struct AdbcStatement* statement,
                                                const char* key, double value,
                                                struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->SetOptionDouble(key, value, error);
}

AdbcStatusCode BigqueryStatementSetOptionInt(struct AdbcStatement* statement,
                                             const char* key, int64_t value,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->SetOptionInt(key, value, error);
}

AdbcStatusCode BigqueryStatementSetSqlQuery(struct AdbcStatement* statement,
                                            const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<BigqueryStatement>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(query, error);
}
}  // namespace

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return BigqueryStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return BigqueryStatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementCancel(struct AdbcStatement* statement,
                                   struct AdbcError* error) {
  return BigqueryStatementCancel(statement, error);
}

AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return BigqueryStatementExecutePartitions(statement, schema, partitions, rows_affected,
                                            error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* output,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return BigqueryStatementExecuteQuery(statement, output, rows_affected, error);
}

AdbcStatusCode AdbcStatementExecuteSchema(struct AdbcStatement* statement,
                                          ArrowSchema* schema, struct AdbcError* error) {
  return BigqueryStatementExecuteSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementGetOption(struct AdbcStatement* statement, const char* key,
                                      char* value, size_t* length,
                                      struct AdbcError* error) {
  return BigqueryStatementGetOption(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, uint8_t* value,
                                           size_t* length, struct AdbcError* error) {
  return BigqueryStatementGetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t* value, struct AdbcError* error) {
  return BigqueryStatementGetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double* value,
                                            struct AdbcError* error) {
  return BigqueryStatementGetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  return BigqueryStatementGetParameterSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return BigqueryStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return BigqueryStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return BigqueryStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return BigqueryStatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, const uint8_t* value,
                                           size_t length, struct AdbcError* error) {
  return BigqueryStatementSetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementSetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t value, struct AdbcError* error) {
  return BigqueryStatementSetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double value,
                                            struct AdbcError* error) {
  return BigqueryStatementSetOptionDouble(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return BigqueryStatementSetSqlQuery(statement, query, error);
}

extern "C" {
ADBC_EXPORT
AdbcStatusCode BigqueryDriverInit(int version, void* raw_driver,
                                    struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0 && version != ADBC_VERSION_1_1_0) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  if (!raw_driver) return ADBC_STATUS_INVALID_ARGUMENT;

  auto* driver = reinterpret_cast<struct AdbcDriver*>(raw_driver);
  if (version >= ADBC_VERSION_1_1_0) {
    std::memset(driver, 0, ADBC_DRIVER_1_1_0_SIZE);

    driver->ErrorGetDetailCount = CommonErrorGetDetailCount;
    driver->ErrorGetDetail = CommonErrorGetDetail;
    driver->ErrorFromArrayStream = BigqueryErrorFromArrayStream;

    driver->DatabaseGetOption = BigqueryDatabaseGetOption;
    driver->DatabaseGetOptionBytes = BigqueryDatabaseGetOptionBytes;
    driver->DatabaseGetOptionDouble = BigqueryDatabaseGetOptionDouble;
    driver->DatabaseGetOptionInt = BigqueryDatabaseGetOptionInt;
    driver->DatabaseSetOptionBytes = BigqueryDatabaseSetOptionBytes;
    driver->DatabaseSetOptionDouble = BigqueryDatabaseSetOptionDouble;
    driver->DatabaseSetOptionInt = BigqueryDatabaseSetOptionInt;

    driver->ConnectionCancel = BigqueryConnectionCancel;
    driver->ConnectionGetOption = BigqueryConnectionGetOption;
    driver->ConnectionGetOptionBytes = BigqueryConnectionGetOptionBytes;
    driver->ConnectionGetOptionDouble = BigqueryConnectionGetOptionDouble;
    driver->ConnectionGetOptionInt = BigqueryConnectionGetOptionInt;
    driver->ConnectionGetStatistics = BigqueryConnectionGetStatistics;
    driver->ConnectionGetStatisticNames = BigqueryConnectionGetStatisticNames;
    driver->ConnectionSetOptionBytes = BigqueryConnectionSetOptionBytes;
    driver->ConnectionSetOptionDouble = BigqueryConnectionSetOptionDouble;
    driver->ConnectionSetOptionInt = BigqueryConnectionSetOptionInt;

    driver->StatementCancel = BigqueryStatementCancel;
    driver->StatementExecuteSchema = BigqueryStatementExecuteSchema;
    driver->StatementGetOption = BigqueryStatementGetOption;
    driver->StatementGetOptionBytes = BigqueryStatementGetOptionBytes;
    driver->StatementGetOptionDouble = BigqueryStatementGetOptionDouble;
    driver->StatementGetOptionInt = BigqueryStatementGetOptionInt;
    driver->StatementSetOptionBytes = BigqueryStatementSetOptionBytes;
    driver->StatementSetOptionDouble = BigqueryStatementSetOptionDouble;
    driver->StatementSetOptionInt = BigqueryStatementSetOptionInt;
  } else {
    std::memset(driver, 0, ADBC_DRIVER_1_0_0_SIZE);
  }

  driver->DatabaseInit = BigqueryDatabaseInit;
  driver->DatabaseNew = BigqueryDatabaseNew;
  driver->DatabaseRelease = BigqueryDatabaseRelease;
  driver->DatabaseSetOption = BigqueryDatabaseSetOption;

  driver->ConnectionCommit = BigqueryConnectionCommit;
  driver->ConnectionGetInfo = BigqueryConnectionGetInfo;
  driver->ConnectionGetObjects = BigqueryConnectionGetObjects;
  driver->ConnectionGetTableSchema = BigqueryConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = BigqueryConnectionGetTableTypes;
  driver->ConnectionInit = BigqueryConnectionInit;
  driver->ConnectionNew = BigqueryConnectionNew;
  driver->ConnectionReadPartition = BigqueryConnectionReadPartition;
  driver->ConnectionRelease = BigqueryConnectionRelease;
  driver->ConnectionRollback = BigqueryConnectionRollback;
  driver->ConnectionSetOption = BigqueryConnectionSetOption;

  driver->StatementBind = BigqueryStatementBind;
  driver->StatementBindStream = BigqueryStatementBindStream;
  driver->StatementExecutePartitions = BigqueryStatementExecutePartitions;
  driver->StatementExecuteQuery = BigqueryStatementExecuteQuery;
  driver->StatementGetParameterSchema = BigqueryStatementGetParameterSchema;
  driver->StatementNew = BigqueryStatementNew;
  driver->StatementPrepare = BigqueryStatementPrepare;
  driver->StatementRelease = BigqueryStatementRelease;
  driver->StatementSetOption = BigqueryStatementSetOption;
  driver->StatementSetSqlQuery = BigqueryStatementSetSqlQuery;

  return ADBC_STATUS_OK;
}

ADBC_EXPORT
AdbcStatusCode AdbcDriverInit(int version, void* driver, struct AdbcError* error) {
  static bool initialized = false;
  if (!initialized) {
    absl::InitializeLog();
    initialized = true;
  }

  return BigqueryDriverInit(version, driver, error);
}
}
