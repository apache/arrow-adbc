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

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <string.h>

#include <adbc.h>

struct VoidDriverPrivate {
  char token[1024];
};

struct VoidDatabasePrivate {
  char token[1024];
};

struct VoidConnectionPrivate {
  char token[1024];
};

struct VoidStatementPrivate {
  char token[1024];
};

static void ResetError(struct AdbcError* error) {
  memset(error, 0, sizeof(struct AdbcError));
}

static void SetErrorConst(struct AdbcError* error, const char* value) {
  if (error == NULL) {
    return;
  }

  ResetError(error);
  error->message = (char*)value;
}

static AdbcStatusCode VoidDriverRelease(struct AdbcDriver* driver,
                                        struct AdbcError* error) {
  if (driver->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(driver->private_data);
  driver->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidDatabaseNew(struct AdbcDatabase* database,
                                      struct AdbcError* error) {
  struct VoidDatabasePrivate* database_private =
      (struct VoidDatabasePrivate*)malloc(sizeof(struct VoidDatabasePrivate));
  if (database_private == NULL) {
    SetErrorConst(error, "failed to allocate VoidDatabasePrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(database_private, 0, sizeof(struct VoidDatabasePrivate));
  database->private_data = database_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidDatabaseInit(struct AdbcDatabase* database,
                                       struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidDatabaseSetOption(struct AdbcDatabase* database,
                                            const char* key, const char* value,
                                            struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidDatabaseRelease(struct AdbcDatabase* database,
                                          struct AdbcError* error) {
  if (database->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(database->private_data);
  database->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidConnectionCommit(struct AdbcConnection* connection,
                                           struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidConnectionGetInfo(struct AdbcConnection* connection,
                                            const uint32_t* info_codes,
                                            size_t info_codes_length,
                                            struct ArrowArrayStream* stream,
                                            struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidConnectionGetTableTypes(struct AdbcConnection* connection,
                                                  struct ArrowArrayStream* stream,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidConnectionInit(struct AdbcConnection* connection,
                                         struct AdbcDatabase* database,
                                         struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidConnectionNew(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  struct VoidConnectionPrivate* connection_private =
      (struct VoidConnectionPrivate*)malloc(sizeof(struct VoidConnectionPrivate));
  if (connection_private == NULL) {
    SetErrorConst(error, "failed to allocate VoidConnectionPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(connection_private, 0, sizeof(struct VoidConnectionPrivate));
  connection->private_data = connection_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidConnectionReadPartition(struct AdbcConnection* connection,
                                                  const uint8_t* serialized_partition,
                                                  size_t serialized_length,
                                                  struct ArrowArrayStream* out,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidConnectionRelease(struct AdbcConnection* connection,
                                            struct AdbcError* error) {
  if (connection->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(connection->private_data);
  connection->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidConnectionRollback(struct AdbcConnection* connection,
                                             struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidConnectionSetOption(struct AdbcConnection* connection,
                                              const char* key, const char* value,
                                              struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidStatementBind(struct AdbcStatement* statement,
                                        struct ArrowArray* values,
                                        struct ArrowSchema* schema,
                                        struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode VoidStatementBindStream(struct AdbcStatement* statement,
                                              struct ArrowArrayStream* stream,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidStatementExecutePartitions(struct AdbcStatement* statement,
                                                     struct ArrowSchema* schema,
                                                     struct AdbcPartitions* partitions,
                                                     int64_t* rows_affected,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode VoidStatementExecuteQuery(struct AdbcStatement* statement,
                                                struct ArrowArrayStream* out,
                                                int64_t* rows_affected,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidStatementGetParameterSchema(struct AdbcStatement* statement,
                                                      struct ArrowSchema* schema,
                                                      struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidStatementNew(struct AdbcConnection* connection,
                                       struct AdbcStatement* statement,
                                       struct AdbcError* error) {
  struct VoidStatementPrivate* statement_private =
      (struct VoidStatementPrivate*)malloc(sizeof(struct VoidStatementPrivate));
  if (statement_private == NULL) {
    SetErrorConst(error, "failed to allocate VoidStatementPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(statement_private, 0, sizeof(struct VoidStatementPrivate));
  statement->private_data = statement_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidStatementPrepare(struct AdbcStatement* statement,
                                           struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidStatementRelease(struct AdbcStatement* statement,
                                           struct AdbcError* error) {
  if (statement->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(statement->private_data);
  statement->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidStatementSetOption(struct AdbcStatement* statement,
                                             const char* key, const char* value,
                                             struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode VoidStatementSetSqlQuery(struct AdbcStatement* statement,
                                               const char* query,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode VoidDriverInitFunc(int version, void* raw_driver,
                                         struct AdbcError* error) {
  if (version != ADBC_VERSION_1_1_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, sizeof(struct AdbcDriver));

  struct VoidDriverPrivate* driver_private =
      (struct VoidDriverPrivate*)malloc(sizeof(struct VoidDriverPrivate));
  if (driver_private == NULL) {
    SetErrorConst(error, "failed to allocate VoidDriverPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(driver_private, 0, sizeof(struct VoidDriverPrivate));
  driver->private_data = driver_private;

  driver->DatabaseInit = &VoidDatabaseInit;
  driver->DatabaseNew = VoidDatabaseNew;
  driver->DatabaseRelease = VoidDatabaseRelease;
  driver->DatabaseSetOption = VoidDatabaseSetOption;

  driver->ConnectionCommit = VoidConnectionCommit;
  driver->ConnectionGetInfo = VoidConnectionGetInfo;
  driver->ConnectionGetObjects = VoidConnectionGetObjects;
  driver->ConnectionGetTableSchema = VoidConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = VoidConnectionGetTableTypes;
  driver->ConnectionInit = VoidConnectionInit;
  driver->ConnectionNew = VoidConnectionNew;
  driver->ConnectionReadPartition = VoidConnectionReadPartition;
  driver->ConnectionRelease = VoidConnectionRelease;
  driver->ConnectionRollback = VoidConnectionRollback;
  driver->ConnectionSetOption = VoidConnectionSetOption;

  driver->StatementBind = VoidStatementBind;
  driver->StatementBindStream = VoidStatementBindStream;
  driver->StatementExecutePartitions = VoidStatementExecutePartitions;
  driver->StatementExecuteQuery = VoidStatementExecuteQuery;
  driver->StatementGetParameterSchema = VoidStatementGetParameterSchema;
  driver->StatementNew = VoidStatementNew;
  driver->StatementPrepare = VoidStatementPrepare;
  driver->StatementRelease = VoidStatementRelease;
  driver->StatementSetOption = VoidStatementSetOption;
  driver->StatementSetSqlQuery = VoidStatementSetSqlQuery;

  driver->release = VoidDriverRelease;

  return ADBC_STATUS_OK;
}

SEXP RAdbcVoidDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)VoidDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}
