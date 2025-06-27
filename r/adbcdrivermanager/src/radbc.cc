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

#include <cstring>
#include <utility>

#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"

#include "radbc.h"

static const char* adbc_error_message(AdbcError* error) {
  if (error->message == nullptr) {
    return "";
  } else {
    return error->message;
  }
}

static void adbc_error_warn(int code, AdbcError* error, const char* context) {
  if (code != ADBC_STATUS_OK) {
    Rf_warning("<%s> %s", context, adbc_error_message(error));
  }
}

static int adbc_update_parent_child_count(SEXP xptr, int delta) {
  SEXP parent_xptr = R_ExternalPtrProtected(xptr);
  if (parent_xptr == R_NilValue) {
    return NA_INTEGER;
  }

  SEXP parent_env = R_ExternalPtrTag(parent_xptr);
  if (parent_env == R_NilValue) {
    return NA_INTEGER;
  }

  SEXP child_count_sexp = Rf_findVarInFrame(parent_env, Rf_install(".child_count"));
  int* child_count = INTEGER(child_count_sexp);
  int old_value = child_count[0];
  child_count[0] = child_count[0] + delta;
  return old_value;
}

static void finalize_driver_xptr(SEXP driver_xptr) {
  auto driver = reinterpret_cast<AdbcDriver*>(R_ExternalPtrAddr(driver_xptr));
  if (driver == nullptr) {
    return;
  }

  if (driver->release != nullptr) {
    AdbcError error = ADBC_ERROR_INIT;
    int status = driver->release(driver, &error);
    adbc_error_warn(status, &error, "finalize_driver_xptr()");
  }

  adbc_xptr_default_finalize<AdbcDriver>(driver_xptr);
  R_SetExternalPtrAddr(driver_xptr, nullptr);
}

static void finalize_database_xptr(SEXP database_xptr) {
  auto database = reinterpret_cast<AdbcDatabase*>(R_ExternalPtrAddr(database_xptr));
  if (database == nullptr) {
    return;
  }

  if (database->private_data != nullptr) {
    AdbcError error = ADBC_ERROR_INIT;
    int status = AdbcDatabaseRelease(database, &error);
    adbc_error_warn(status, &error, "finalize_database_xptr()");
  }

  adbc_xptr_default_finalize<AdbcDatabase>(database_xptr);
}

extern "C" SEXP RAdbcAllocateDriver(void) {
  SEXP driver_xptr = PROTECT(adbc_allocate_xptr<AdbcDriver>());
  R_RegisterCFinalizer(driver_xptr, &finalize_driver_xptr);

  // Make sure we error when the ADBC spec is updated
  static_assert(sizeof(AdbcDriver) == ADBC_DRIVER_1_1_0_SIZE);
  SEXP version_sexp = PROTECT(Rf_ScalarInteger(ADBC_VERSION_1_1_0));

  const char* names[] = {"driver", "version", ""};
  SEXP out = PROTECT(Rf_mkNamed(VECSXP, names));
  SET_VECTOR_ELT(out, 0, driver_xptr);
  SET_VECTOR_ELT(out, 1, version_sexp);
  UNPROTECT(3);
  return out;
}

extern "C" SEXP RAdbcLoadDriver(SEXP driver_name_sexp, SEXP entrypoint_sexp,
                                SEXP version_sexp, SEXP load_flags_sexp, SEXP driver_sexp,
                                SEXP error_sexp) {
  const char* driver_name = adbc_as_const_char(driver_name_sexp);
  const char* entrypoint = adbc_as_const_char(entrypoint_sexp, /*nullable*/ true);
  int version = adbc_as_int(version_sexp);
  int load_flags = adbc_as_int(load_flags_sexp);

  if (TYPEOF(driver_sexp) != EXTPTRSXP) {
    Rf_error("driver must be an externalptr");
  }
  void* driver = R_ExternalPtrAddr(driver_sexp);

  AdbcError* error;
  if (error_sexp == R_NilValue) {
    error = nullptr;
  } else if (TYPEOF(error_sexp) == EXTPTRSXP) {
    error = reinterpret_cast<AdbcError*>(R_ExternalPtrAddr(error_sexp));
  } else {
    Rf_error("error must be an externalptr");
  }

  int status =
      AdbcFindLoadDriver(driver_name, entrypoint, version, load_flags, driver, error);
  return Rf_ScalarInteger(status);
}

extern "C" SEXP RAdbcLoadDriverFromInitFunc(SEXP driver_init_func_xptr, SEXP version_sexp,
                                            SEXP driver_sexp, SEXP error_sexp) {
  if (!Rf_inherits(driver_init_func_xptr, "adbc_driver_init_func")) {
    Rf_error("Expected external pointer with class '%s'", "adbc_driver_init_func");
  }

  auto driver_init_func =
      reinterpret_cast<AdbcDriverInitFunc>(R_ExternalPtrAddrFn(driver_init_func_xptr));

  int version = adbc_as_int(version_sexp);

  if (TYPEOF(driver_sexp) != EXTPTRSXP) {
    Rf_error("driver must be an externalptr");
  }
  void* driver = R_ExternalPtrAddr(driver_sexp);

  AdbcError* error;
  if (error_sexp == R_NilValue) {
    error = nullptr;
  } else if (TYPEOF(error_sexp) == EXTPTRSXP) {
    error = reinterpret_cast<AdbcError*>(R_ExternalPtrAddr(error_sexp));
  } else {
    Rf_error("error must be an externalptr");
  }

  int status = AdbcLoadDriverFromInitFunc(driver_init_func, version, driver, error);
  return Rf_ScalarInteger(status);
}

extern "C" SEXP RAdbcDatabaseNew(SEXP driver_init_func_xptr) {
  SEXP database_xptr = PROTECT(adbc_allocate_xptr<AdbcDatabase>());
  R_RegisterCFinalizer(database_xptr, &finalize_database_xptr);

  AdbcDatabase* database = adbc_from_xptr<AdbcDatabase>(database_xptr);

  AdbcError error = ADBC_ERROR_INIT;
  int status = AdbcDatabaseNew(database, &error);
  adbc_error_stop(status, &error);

  if (driver_init_func_xptr != R_NilValue) {
    if (!Rf_inherits(driver_init_func_xptr, "adbc_driver_init_func")) {
      Rf_error("Expected external pointer with class '%s'", "adbc_driver_init_func");
    }

    auto driver_init_func =
        reinterpret_cast<AdbcDriverInitFunc>(R_ExternalPtrAddrFn(driver_init_func_xptr));

    status = AdbcDriverManagerDatabaseSetInitFunc(database, driver_init_func, &error);
    adbc_error_stop(status, &error);
  }

  UNPROTECT(1);
  return database_xptr;
}

extern "C" SEXP RAdbcMoveDatabase(SEXP database_xptr) {
  AdbcDatabase* database = adbc_from_xptr<AdbcDatabase>(database_xptr);
  SEXP database_xptr_new = PROTECT(adbc_allocate_xptr<AdbcDatabase>());
  R_RegisterCFinalizer(database_xptr_new, &finalize_database_xptr);
  AdbcDatabase* database_new = adbc_from_xptr<AdbcDatabase>(database_xptr_new);

  std::memcpy(database_new, database, sizeof(AdbcDatabase));
  adbc_xptr_move_attrs(database_xptr, database_xptr_new);
  std::memset(database, 0, sizeof(AdbcDatabase));

  UNPROTECT(1);
  return database_xptr_new;
}

extern "C" SEXP RAdbcDatabaseValid(SEXP database_xptr) {
  AdbcDatabase* database = adbc_from_xptr<AdbcDatabase>(database_xptr, /*nullable=*/true);
  return Rf_ScalarLogical(database != nullptr && database->private_data != nullptr);
}

extern "C" SEXP RAdbcDatabaseInit(SEXP database_xptr, SEXP error_xptr) {
  auto database = adbc_from_xptr<AdbcDatabase>(database_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  return adbc_wrap_status(AdbcDatabaseInit(database, error));
}

extern "C" SEXP RAdbcDatabaseRelease(SEXP database_xptr, SEXP error_xptr) {
  auto database = adbc_from_xptr<AdbcDatabase>(database_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcDatabaseRelease(database, error);
  return adbc_wrap_status(status);
}

static void finalize_connection_xptr(SEXP connection_xptr) {
  auto connection = reinterpret_cast<AdbcConnection*>(R_ExternalPtrAddr(connection_xptr));
  if (connection == nullptr) {
    return;
  }

  if (connection->private_data != nullptr) {
    AdbcError error = ADBC_ERROR_INIT;
    int status = AdbcConnectionRelease(connection, &error);
    adbc_error_warn(status, &error, "finalize_connection_xptr()");
    if (status == ADBC_STATUS_OK) {
      adbc_update_parent_child_count(connection_xptr, -1);
    }
  }

  adbc_xptr_default_finalize<AdbcConnection>(connection_xptr);
}

extern "C" SEXP RAdbcConnectionNew(void) {
  SEXP connection_xptr = PROTECT(adbc_allocate_xptr<AdbcConnection>());
  R_RegisterCFinalizer(connection_xptr, &finalize_connection_xptr);

  AdbcConnection* connection = adbc_from_xptr<AdbcConnection>(connection_xptr);

  AdbcError error = ADBC_ERROR_INIT;
  int status = AdbcConnectionNew(connection, &error);
  adbc_error_stop(status, &error);

  UNPROTECT(1);
  return connection_xptr;
}

extern "C" SEXP RAdbcMoveConnection(SEXP connection_xptr) {
  AdbcConnection* connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  SEXP connection_xptr_new = PROTECT(adbc_allocate_xptr<AdbcConnection>());
  R_RegisterCFinalizer(connection_xptr_new, &finalize_connection_xptr);
  AdbcConnection* connection_new = adbc_from_xptr<AdbcConnection>(connection_xptr_new);

  std::memcpy(connection_new, connection, sizeof(AdbcConnection));
  adbc_xptr_move_attrs(connection_xptr, connection_xptr_new);
  std::memset(connection, 0, sizeof(AdbcConnection));

  UNPROTECT(1);
  return connection_xptr_new;
}

extern "C" SEXP RAdbcConnectionValid(SEXP connection_xptr) {
  AdbcConnection* connection =
      adbc_from_xptr<AdbcConnection>(connection_xptr, /*nullable=*/true);
  return Rf_ScalarLogical(connection != nullptr && connection->private_data != nullptr);
}

extern "C" SEXP RAdbcConnectionInit(SEXP connection_xptr, SEXP database_xptr,
                                    SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto database = adbc_from_xptr<AdbcDatabase>(database_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int result = AdbcConnectionInit(connection, database, error);
  if (result == ADBC_STATUS_OK) {
    // Keep the database pointer alive for as long as the connection pointer
    // is alive
    R_SetExternalPtrProtected(connection_xptr, database_xptr);
    adbc_update_parent_child_count(connection_xptr, 1);
  }

  return adbc_wrap_status(result);
}

extern "C" SEXP RAdbcConnectionRelease(SEXP connection_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcConnectionRelease(connection, error);
  if (status == ADBC_STATUS_OK) {
    adbc_update_parent_child_count(connection_xptr, -1);
  }

  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionGetInfo(SEXP connection_xptr, SEXP info_codes_sexp,
                                       SEXP out_stream_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  auto out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  std::pair<SEXP, int*> info_codes = adbc_as_int_list(info_codes_sexp);
  PROTECT(info_codes.first);
  size_t info_codes_length = Rf_xlength(info_codes_sexp);
  int status =
      AdbcConnectionGetInfo(connection, reinterpret_cast<uint32_t*>(info_codes.second),
                            info_codes_length, out_stream, error);
  UNPROTECT(1);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionGetObjects(SEXP connection_xptr, SEXP depth_sexp,
                                          SEXP catalog_sexp, SEXP db_schema_sexp,
                                          SEXP table_name_sexp, SEXP table_type_sexp,
                                          SEXP column_name_sexp, SEXP out_stream_xptr,
                                          SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  int depth = adbc_as_int(depth_sexp);
  const char* catalog = adbc_as_const_char(catalog_sexp, /*nullable=*/true);
  const char* db_schema = adbc_as_const_char(db_schema_sexp, /*nullable=*/true);
  const char* table_name = adbc_as_const_char(table_name_sexp, /*nullable=*/true);
  std::pair<SEXP, const char**> table_type = adbc_as_const_char_list(table_type_sexp);
  PROTECT(table_type.first);

  const char* column_name = adbc_as_const_char(column_name_sexp, /*nullable=*/true);
  auto out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status =
      AdbcConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
                               table_type.second, column_name, out_stream, error);
  UNPROTECT(1);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionGetTableSchema(SEXP connection_xptr, SEXP catalog_sexp,
                                              SEXP db_schema_sexp, SEXP table_name_sexp,
                                              SEXP schema_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  const char* catalog = adbc_as_const_char(catalog_sexp, /*nullable=*/true);
  const char* db_schema = adbc_as_const_char(db_schema_sexp, /*nullable=*/true);
  const char* table_name = adbc_as_const_char(table_name_sexp);
  auto schema = adbc_from_xptr<ArrowSchema>(schema_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcConnectionGetTableSchema(connection, catalog, db_schema, table_name,
                                            schema, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionGetTableTypes(SEXP connection_xptr, SEXP out_stream_xptr,
                                             SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcConnectionGetTableTypes(connection, out_stream, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionReadPartition(SEXP connection_xptr,
                                             SEXP serialized_partition_sexp,
                                             SEXP out_stream_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto serialized_partition = reinterpret_cast<const uint8_t*>(serialized_partition_sexp);
  uint32_t serialized_length = Rf_xlength(serialized_partition_sexp);
  auto out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcConnectionReadPartition(connection, serialized_partition,
                                           serialized_length, out_stream, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionCommit(SEXP connection_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcConnectionCommit(connection, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionRollback(SEXP connection_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcConnectionRollback(connection, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionCancel(SEXP connection_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcConnectionCancel(connection, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionGetStatisticNames(SEXP connection_xptr,
                                                 SEXP out_stream_xptr, SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  auto out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcConnectionGetStatisticNames(connection, out_stream, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcConnectionGetStatistics(SEXP connection_xptr, SEXP catalog_sexp,
                                             SEXP db_schema_sexp, SEXP table_name_sexp,
                                             SEXP approximate_sexp, SEXP out_stream_xptr,
                                             SEXP error_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  const char* catalog = adbc_as_const_char(catalog_sexp, /*nullable=*/true);
  const char* db_schema = adbc_as_const_char(db_schema_sexp, /*nullable=*/true);
  const char* table_name = adbc_as_const_char(table_name_sexp);
  char approximate = adbc_as_bool(approximate_sexp);
  auto out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcConnectionGetStatistics(connection, catalog, db_schema, table_name,
                                           approximate, out_stream, error);
  return adbc_wrap_status(status);
}

static void finalize_statement_xptr(SEXP statement_xptr) {
  auto statement = reinterpret_cast<AdbcStatement*>(R_ExternalPtrAddr(statement_xptr));
  if (statement == nullptr) {
    return;
  }

  if (statement->private_data != nullptr) {
    AdbcError error = ADBC_ERROR_INIT;
    int status = AdbcStatementRelease(statement, &error);
    adbc_error_warn(status, &error, "finalize_statement_xptr()");
    if (status == ADBC_STATUS_OK) {
      adbc_update_parent_child_count(statement_xptr, -1);
    }
  }

  adbc_xptr_default_finalize<AdbcStatement>(statement_xptr);
}

extern "C" SEXP RAdbcStatementNew(SEXP connection_xptr) {
  auto connection = adbc_from_xptr<AdbcConnection>(connection_xptr);
  SEXP statement_xptr = PROTECT(adbc_allocate_xptr<AdbcStatement>(connection_xptr));
  R_RegisterCFinalizer(statement_xptr, &finalize_statement_xptr);

  AdbcStatement* statement = adbc_from_xptr<AdbcStatement>(statement_xptr);

  AdbcError error = ADBC_ERROR_INIT;
  int status = AdbcStatementNew(connection, statement, &error);
  adbc_error_stop(status, &error);

  R_SetExternalPtrProtected(statement_xptr, connection_xptr);
  adbc_update_parent_child_count(statement_xptr, 1);

  UNPROTECT(1);
  return statement_xptr;
}

extern "C" SEXP RAdbcMoveStatement(SEXP statement_xptr) {
  AdbcStatement* statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  SEXP statement_xptr_new = PROTECT(adbc_allocate_xptr<AdbcStatement>());
  R_RegisterCFinalizer(statement_xptr_new, &finalize_statement_xptr);
  AdbcStatement* statement_new = adbc_from_xptr<AdbcStatement>(statement_xptr_new);

  std::memcpy(statement_new, statement, sizeof(AdbcStatement));
  adbc_xptr_move_attrs(statement_xptr, statement_xptr_new);
  std::memset(statement, 0, sizeof(AdbcStatement));

  UNPROTECT(1);
  return statement_xptr_new;
}

extern "C" SEXP RAdbcStatementValid(SEXP statement_xptr) {
  AdbcStatement* statement =
      adbc_from_xptr<AdbcStatement>(statement_xptr, /*nullable=*/true);
  return Rf_ScalarLogical(statement != nullptr && statement->private_data != nullptr);
}

extern "C" SEXP RAdbcStatementRelease(SEXP statement_xptr, SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcStatementRelease(statement, error);
  if (status == ADBC_STATUS_OK) {
    adbc_update_parent_child_count(statement_xptr, -1);
  }

  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementSetSqlQuery(SEXP statement_xptr, SEXP query_sexp,
                                          SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  const char* query = adbc_as_const_char(query_sexp);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcStatementSetSqlQuery(statement, query, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementSetSubstraitPlan(SEXP statement_xptr, SEXP plan_sexp,
                                               SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto plan = reinterpret_cast<uint8_t*>(RAW(plan_sexp));
  size_t plan_length = Rf_xlength(plan_sexp);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcStatementSetSubstraitPlan(statement, plan, plan_length, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementPrepare(SEXP statement_xptr, SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcStatementPrepare(statement, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementGetParameterSchema(SEXP statement_xptr,
                                                 SEXP out_schema_xptr, SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto out_schema = adbc_from_xptr<ArrowSchema>(out_schema_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcStatementGetParameterSchema(statement, out_schema, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementBind(SEXP statement_xptr, SEXP values_xptr,
                                   SEXP schema_xptr, SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto values = adbc_from_xptr<ArrowArray>(values_xptr);
  auto schema = adbc_from_xptr<ArrowSchema>(schema_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcStatementBind(statement, values, schema, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementBindStream(SEXP statement_xptr, SEXP stream_xptr,
                                         SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto stream = adbc_from_xptr<ArrowArrayStream>(stream_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcStatementBindStream(statement, stream, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementExecuteQuery(SEXP statement_xptr, SEXP out_stream_xptr,
                                           SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);

  ArrowArrayStream* out_stream;
  if (out_stream_xptr == R_NilValue) {
    out_stream = nullptr;
  } else {
    out_stream = adbc_from_xptr<ArrowArrayStream>(out_stream_xptr);
  }

  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int64_t rows_affected = -1;
  int status = AdbcStatementExecuteQuery(statement, out_stream, &rows_affected, error);

  const char* names[] = {"status", "rows_affected", ""};
  SEXP result = PROTECT(Rf_mkNamed(VECSXP, names));

  SEXP status_sexp = PROTECT(adbc_wrap_status(status));
  SET_VECTOR_ELT(result, 0, status_sexp);
  UNPROTECT(1);

  SEXP rows_affected_sexp = PROTECT(Rf_ScalarReal(rows_affected));
  SET_VECTOR_ELT(result, 1, rows_affected_sexp);
  UNPROTECT(2);
  return result;
}

extern "C" SEXP RAdbcStatementExecuteSchema(SEXP statement_xptr, SEXP out_schema_xptr,
                                            SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto out_schema = adbc_from_xptr<ArrowSchema>(out_schema_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = AdbcStatementExecuteSchema(statement, out_schema, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcStatementExecutePartitions(SEXP statement_xptr, SEXP out_schema_xptr,
                                                SEXP partitions_xptr, SEXP error_xptr) {
  return adbc_wrap_status(ADBC_STATUS_NOT_IMPLEMENTED);
}

extern "C" SEXP RAdbcStatementCancel(SEXP statement_xptr, SEXP error_xptr) {
  auto statement = adbc_from_xptr<AdbcStatement>(statement_xptr);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  int status = AdbcStatementCancel(statement, error);
  return adbc_wrap_status(status);
}
