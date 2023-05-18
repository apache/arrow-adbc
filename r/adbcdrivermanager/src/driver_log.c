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
#include <adbc_driver_manager.h>

struct LogDriverPrivate {
  char token[1024];
};

struct LogDatabasePrivate {
  char* tag;
  struct AdbcDatabase parent_database;
};

struct LogConnectionPrivate {
  const char* tag;
  struct AdbcConnection parent_connection;
};

struct LogStatementPrivate {
  const char* tag;
  struct AdbcStatement parent_statement;
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

static AdbcStatusCode LogDriverRelease(struct AdbcDriver* driver,
                                       struct AdbcError* error) {
  if (driver->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(driver->private_data);
  driver->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogDatabaseNew(struct AdbcDatabase* database,
                                     struct AdbcError* error) {
  struct LogDatabasePrivate* database_private =
      (struct LogDatabasePrivate*)malloc(sizeof(struct LogDatabasePrivate));
  if (database_private == NULL) {
    SetErrorConst(error, "failed to allocate LogDatabasePrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(database_private, 0, sizeof(struct LogDatabasePrivate));
  database->private_data = database_private;

  int result = AdbcDatabaseNew(&database_private->parent_database, &error);
  if (result != ADBC_STATUS_OK) {
    free(database_private);
    database_private = NULL;
  }

  return result;
}

static AdbcStatusCode LogDatabaseInit(struct AdbcDatabase* database,
                                      struct AdbcError* error) {
  struct LogDatabasePrivate* database_private =
      (struct LogDatabasePrivate*)database->private_data;

  Rprintf("%s: AdbcDatabaseInit() ", database_private->tag);
  int result = AdbcDatabaseInit(&database_private->parent_database, error);
  Rprintf("::%d::\n", result);
  return result;
}

static AdbcStatusCode LogDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                           const char* value, struct AdbcError* error) {
  struct LogDatabasePrivate* database_private =
      (struct LogDatabasePrivate*)database->private_data;

  if (strcmp(key, "adbc.r.logdriver.tag") == 0) {
    database_private->tag = malloc(strlen(value) + 1);
    if (database_private->tag == NULL) {
      SetErrorConst(error, "failed to allocate tag");
      return ADBC_STATUS_INTERNAL;
    }
    memcpy(database_private->tag, value, strlen(value) + 1);
    return ADBC_STATUS_OK;
  } else if (strcmp(key, "adbc.r.logdriver.driver_init_func_addr") == 0) {
    char* endptr;
    intptr_t ptr = strtoll(value, &endptr, 10);
    AdbcDriverInitFunc init_func = (AdbcDriverInitFunc)ptr;
    return AdbcDriverManagerDatabaseSetInitFunc(&database_private->parent_database,
                                                init_func, error);
  }

  Rprintf("%s: LogDatabaseSetOption() ", database_private->tag);
  int result =
      AdbcDatabaseSetOption(&database_private->parent_database, key, value, error);
  Rprintf("::%d::\n", result);
  return result;
}

static AdbcStatusCode LogDatabaseRelease(struct AdbcDatabase* database,
                                         struct AdbcError* error) {
  struct LogDatabasePrivate* database_private =
      (struct LogDatabasePrivate*)database->private_data;

  if (database_private == NULL) {
    return ADBC_STATUS_OK;
  }

  Rprintf("%s: AdbcDatabaseRelease() ", database_private->tag);
  int result = AdbcDatabaseRelease(&database_private->parent_database, error);

  if (database_private->tag != NULL) {
    free(database_private->tag);
  }

  free(database->private_data);
  database->private_data = NULL;

  Rprintf("::%d::\n", result);
  return result;
}

static AdbcStatusCode LogConnectionCommit(struct AdbcConnection* connection,
                                          struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogConnectionGetInfo(struct AdbcConnection* connection,
                                           uint32_t* info_codes, size_t info_codes_length,
                                           struct ArrowArrayStream* stream,
                                           struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* stream, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogConnectionGetTableSchema(
    struct AdbcConnection* connection, const char* catalog, const char* db_schema,
    const char* table_name, struct ArrowSchema* schema, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogConnectionGetTableTypes(struct AdbcConnection* connection,
                                                 struct ArrowArrayStream* stream,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogConnectionInit(struct AdbcConnection* connection,
                                        struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogConnectionNew(struct AdbcConnection* connection,
                                       struct AdbcError* error) {
  struct LogConnectionPrivate* connection_private =
      (struct LogConnectionPrivate*)malloc(sizeof(struct LogConnectionPrivate));
  if (connection_private == NULL) {
    SetErrorConst(error, "failed to allocate LogConnectionPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(connection_private, 0, sizeof(struct LogConnectionPrivate));
  connection->private_data = connection_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogConnectionReadPartition(struct AdbcConnection* connection,
                                                 const uint8_t* serialized_partition,
                                                 size_t serialized_length,
                                                 struct ArrowArrayStream* out,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogConnectionRelease(struct AdbcConnection* connection,
                                           struct AdbcError* error) {
  if (connection->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(connection->private_data);
  connection->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogConnectionRollback(struct AdbcConnection* connection,
                                            struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogConnectionSetOption(struct AdbcConnection* connection,
                                             const char* key, const char* value,
                                             struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogStatementBind(struct AdbcStatement* statement,
                                       struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode LogStatementBindStream(struct AdbcStatement* statement,
                                             struct ArrowArrayStream* stream,
                                             struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogStatementExecutePartitions(struct AdbcStatement* statement,
                                                    struct ArrowSchema* schema,
                                                    struct AdbcPartitions* partitions,
                                                    int64_t* rows_affected,
                                                    struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

static AdbcStatusCode LogStatementExecuteQuery(struct AdbcStatement* statement,
                                               struct ArrowArrayStream* out,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogStatementGetParameterSchema(struct AdbcStatement* statement,
                                                     struct ArrowSchema* schema,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogStatementNew(struct AdbcConnection* connection,
                                      struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  struct LogStatementPrivate* statement_private =
      (struct LogStatementPrivate*)malloc(sizeof(struct LogStatementPrivate));
  if (statement_private == NULL) {
    SetErrorConst(error, "failed to allocate LogStatementPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(statement_private, 0, sizeof(struct LogStatementPrivate));
  statement->private_data = statement_private;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogStatementPrepare(struct AdbcStatement* statement,
                                          struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogStatementRelease(struct AdbcStatement* statement,
                                          struct AdbcError* error) {
  if (statement->private_data == NULL) {
    return ADBC_STATUS_OK;
  }

  free(statement->private_data);
  statement->private_data = NULL;
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogStatementSetOption(struct AdbcStatement* statement,
                                            const char* key, const char* value,
                                            struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

static AdbcStatusCode LogStatementSetSqlQuery(struct AdbcStatement* statement,
                                              const char* query,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

static AdbcStatusCode LogDriverInitFunc(int version, void* raw_driver,
                                        struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) return ADBC_STATUS_NOT_IMPLEMENTED;
  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, sizeof(struct AdbcDriver));

  struct LogDriverPrivate* driver_private =
      (struct LogDriverPrivate*)malloc(sizeof(struct LogDriverPrivate));
  if (driver_private == NULL) {
    SetErrorConst(error, "failed to allocate LogDriverPrivate");
    return ADBC_STATUS_INTERNAL;
  }

  memset(driver_private, 0, sizeof(struct LogDriverPrivate));
  driver->private_data = driver_private;

  driver->DatabaseInit = &LogDatabaseInit;
  driver->DatabaseNew = LogDatabaseNew;
  driver->DatabaseRelease = LogDatabaseRelease;
  driver->DatabaseSetOption = LogDatabaseSetOption;

  driver->ConnectionCommit = LogConnectionCommit;
  driver->ConnectionGetInfo = LogConnectionGetInfo;
  driver->ConnectionGetObjects = LogConnectionGetObjects;
  driver->ConnectionGetTableSchema = LogConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = LogConnectionGetTableTypes;
  driver->ConnectionInit = LogConnectionInit;
  driver->ConnectionNew = LogConnectionNew;
  driver->ConnectionReadPartition = LogConnectionReadPartition;
  driver->ConnectionRelease = LogConnectionRelease;
  driver->ConnectionRollback = LogConnectionRollback;
  driver->ConnectionSetOption = LogConnectionSetOption;

  driver->StatementBind = LogStatementBind;
  driver->StatementBindStream = LogStatementBindStream;
  driver->StatementExecutePartitions = LogStatementExecutePartitions;
  driver->StatementExecuteQuery = LogStatementExecuteQuery;
  driver->StatementGetParameterSchema = LogStatementGetParameterSchema;
  driver->StatementNew = LogStatementNew;
  driver->StatementPrepare = LogStatementPrepare;
  driver->StatementRelease = LogStatementRelease;
  driver->StatementSetOption = LogStatementSetOption;
  driver->StatementSetSqlQuery = LogStatementSetSqlQuery;

  driver->release = LogDriverRelease;

  return ADBC_STATUS_OK;
}

SEXP RAdbcLogDriverInitFunc() {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)LogDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}
