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

struct MonkeyDriverPrivate {
  char token[1024];
};

struct MonkeyDatabasePrivate {
  char token[1024];
};

struct MonkeyConnectionPrivate {
  char token[1024];
};

struct MonkeyStatementPrivate {
  struct ArrowArrayStream stream;
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

static AdbcStatusCode MonkeyDriverRelease(struct AdbcDriver* driver,
                                          struct AdbcError* error) {
  if (driver->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(driver->private_data);
  driver->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyDatabaseNew(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  struct MonkeyDatabasePrivate* database_private =
      (struct MonkeyDatabasePrivate*)malloc(sizeof(struct MonkeyDatabasePrivate));
  if (database_private == NULL) {
    SetErrorConst(error, "failed to allocate MonkeyDatabasePrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(database_private, 0, sizeof(struct MonkeyDatabasePrivate));
  database->private_data = database_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyDatabaseInit(struct AdbcDatabase* database,
                                         struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyDatabaseSetOption(struct AdbcDatabase* database,
                                              const char* key, const char* value,
                                              struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyDatabaseRelease(struct AdbcDatabase* database,
                                            struct AdbcError* error) {
  if (database->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(database->private_data);
  database->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyConnectionCommit(struct AdbcConnection* connection,
                                             struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyConnectionGetInfo(struct AdbcConnection* connection,
                                              const uint32_t* info_codes,
                                              size_t info_codes_length,
                                              struct ArrowArrayStream* stream,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyConnectionGetTableTypes(struct AdbcConnection* connection,
                                                    struct ArrowArrayStream* stream,
                                                    struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyConnectionInit(struct AdbcConnection* connection,
                                           struct AdbcDatabase* database,
                                           struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyConnectionNew(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  struct MonkeyConnectionPrivate* connection_private =
      (struct MonkeyConnectionPrivate*)malloc(sizeof(struct MonkeyConnectionPrivate));
  if (connection_private == NULL) {
    SetErrorConst(error, "failed to allocate MonkeyConnectionPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(connection_private, 0, sizeof(struct MonkeyConnectionPrivate));
  connection->private_data = connection_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyConnectionReadPartition(struct AdbcConnection* connection,
                                                    const uint8_t* serialized_partition,
                                                    size_t serialized_length,
                                                    struct ArrowArrayStream* out,
                                                    struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyConnectionRelease(struct AdbcConnection* connection,
                                              struct AdbcError* error) {
  if (connection->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(connection->private_data);
  connection->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyConnectionRollback(struct AdbcConnection* connection,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyConnectionSetOption(struct AdbcConnection* connection,
                                                const char* key, const char* value,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyStatementBind(struct AdbcStatement* statement,
                                          struct ArrowArray* values,
                                          struct ArrowSchema* schema,
                                          struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode MonkeyStatementBindStream(struct AdbcStatement* statement,
                                                struct ArrowArrayStream* stream,
                                                struct AdbcError* error) {
  struct MonkeyStatementPrivate* statement_private =
      (struct MonkeyStatementPrivate*)statement->private_data;
  if (statement_private->stream.release != NULL) {
    statement_private->stream.release(&statement_private->stream);
  }

  memcpy(&statement_private->stream, stream, sizeof(struct ArrowArrayStream));
  stream->release = NULL;

  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyStatementExecutePartitions(struct AdbcStatement* statement,
                                                       struct ArrowSchema* schema,
                                                       struct AdbcPartitions* partitions,
                                                       int64_t* rows_affected,
                                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode MonkeyStatementExecuteQuery(struct AdbcStatement* statement,
                                                  struct ArrowArrayStream* out,
                                                  int64_t* rows_affected,
                                                  struct AdbcError* error) {
  if (out == NULL) {
    *rows_affected = 0;
    return ADBC_STATUS_OK;
  }

  struct MonkeyStatementPrivate* statement_private =
      (struct MonkeyStatementPrivate*)statement->private_data;

  memcpy(out, &statement_private->stream, sizeof(struct ArrowArrayStream));
  statement_private->stream.release = NULL;
  *rows_affected = -1;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyStatementGetParameterSchema(struct AdbcStatement* statement,
                                                        struct ArrowSchema* schema,
                                                        struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyStatementNew(struct AdbcConnection* connection,
                                         struct AdbcStatement* statement,
                                         struct AdbcError* error) {
  struct MonkeyStatementPrivate* statement_private =
      (struct MonkeyStatementPrivate*)malloc(sizeof(struct MonkeyStatementPrivate));
  if (statement_private == NULL) {
    SetErrorConst(error, "failed to allocate MonkeyStatementPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(statement_private, 0, sizeof(struct MonkeyStatementPrivate));
  statement->private_data = statement_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyStatementPrepare(struct AdbcStatement* statement,
                                             struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyStatementRelease(struct AdbcStatement* statement,
                                             struct AdbcError* error) {
  if (statement->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  struct MonkeyStatementPrivate* statement_private =
      (struct MonkeyStatementPrivate*)statement->private_data;
  if (statement_private->stream.release != NULL) {
    statement_private->stream.release(&statement_private->stream);
  }

  free(statement->private_data);
  statement->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyStatementSetOption(struct AdbcStatement* statement,
                                               const char* key, const char* value,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode MonkeyStatementSetSqlQuery(struct AdbcStatement* statement,
                                                 const char* query,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode MonkeyDriverInitFunc(int version, void* raw_driver,
                                           struct AdbcError* error) {
  if (version != ADBC_VERSION_1_1_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, sizeof(struct AdbcDriver));

  struct MonkeyDriverPrivate* driver_private =
      (struct MonkeyDriverPrivate*)malloc(sizeof(struct MonkeyDriverPrivate));
  if (driver_private == NULL) {
    SetErrorConst(error, "failed to allocate MonkeyDriverPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(driver_private, 0, sizeof(struct MonkeyDriverPrivate));
  driver->private_data = driver_private;

  driver->DatabaseInit = &MonkeyDatabaseInit;
  driver->DatabaseNew = MonkeyDatabaseNew;
  driver->DatabaseRelease = MonkeyDatabaseRelease;
  driver->DatabaseSetOption = MonkeyDatabaseSetOption;

  driver->ConnectionCommit = MonkeyConnectionCommit;
  driver->ConnectionGetInfo = MonkeyConnectionGetInfo;
  driver->ConnectionGetObjects = MonkeyConnectionGetObjects;
  driver->ConnectionGetTableSchema = MonkeyConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = MonkeyConnectionGetTableTypes;
  driver->ConnectionInit = MonkeyConnectionInit;
  driver->ConnectionNew = MonkeyConnectionNew;
  driver->ConnectionReadPartition = MonkeyConnectionReadPartition;
  driver->ConnectionRelease = MonkeyConnectionRelease;
  driver->ConnectionRollback = MonkeyConnectionRollback;
  driver->ConnectionSetOption = MonkeyConnectionSetOption;

  driver->StatementBind = MonkeyStatementBind;
  driver->StatementBindStream = MonkeyStatementBindStream;
  driver->StatementExecutePartitions = MonkeyStatementExecutePartitions;
  driver->StatementExecuteQuery = MonkeyStatementExecuteQuery;
  driver->StatementGetParameterSchema = MonkeyStatementGetParameterSchema;
  driver->StatementNew = MonkeyStatementNew;
  driver->StatementPrepare = MonkeyStatementPrepare;
  driver->StatementRelease = MonkeyStatementRelease;
  driver->StatementSetOption = MonkeyStatementSetOption;
  driver->StatementSetSqlQuery = MonkeyStatementSetSqlQuery;

  driver->release = MonkeyDriverRelease;

  return ADBC_STATUS_OK;
}

SEXP RAdbcMonkeyDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)MonkeyDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}
