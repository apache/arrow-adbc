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

#include "driver_base.h"

#include <adbc.h>

using adbc::common::ConnectionObjectBase;
using adbc::common::DatabaseObjectBase;
using adbc::common::Option;
using adbc::common::StatementObjectBase;

using VoidDriver =
    adbc::common::Driver<DatabaseObjectBase, ConnectionObjectBase, StatementObjectBase>;

static AdbcStatusCode VoidDriverInitFunc(int version, void* raw_driver,
                                         AdbcError* error) {
  return VoidDriver::Init(version, raw_driver, error);
}

extern "C" SEXP RAdbcVoidDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)VoidDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}

class MonkeyDriverStatement : public StatementObjectBase {
 public:
  MonkeyDriverStatement() { stream_.release = nullptr; }

  ~MonkeyDriverStatement() {
    if (stream_.release != nullptr) {
      stream_.release(&stream_);
    }
  }

  AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    if (stream_.release != nullptr) {
      stream_.release(&stream_);
    }

    std::memcpy(&stream_, stream, sizeof(ArrowArrayStream));
    stream->release = nullptr;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                              AdbcError* error) {
    if (stream != nullptr) {
      std::memcpy(stream, &stream_, sizeof(ArrowArrayStream));
      stream_.release = nullptr;
    }

    if (rows_affected != nullptr) {
      *rows_affected = -1;
    }

    return ADBC_STATUS_OK;
  }

 private:
  ArrowArrayStream stream_;
};

using MonkeyDriver =
    adbc::common::Driver<DatabaseObjectBase, ConnectionObjectBase, MonkeyDriverStatement>;

static AdbcStatusCode MonkeyDriverInitFunc(int version, void* raw_driver,
                                           AdbcError* error) {
  return MonkeyDriver::Init(version, raw_driver, error);
}

extern "C" SEXP RAdbcMonkeyDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)MonkeyDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}

class LogDriverDatabase : public DatabaseObjectBase {
 public:
  LogDriverDatabase() { Rprintf("LogDatabaseNew()\n"); }

  ~LogDriverDatabase() { Rprintf("LogDatabaseRelease()\n"); }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    Rprintf("LogDatabaseInit()\n");
    return DatabaseObjectBase::Init(parent, error);
  }

  const Option& GetOption(const std::string& key,
                          const Option& default_value = Option()) const {
    Rprintf("LogDatabaseGetOption()\n");
    return DatabaseObjectBase::GetOption(key, default_value);
  }

  AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    Rprintf("LogDatabaseSetOption()\n");
    return DatabaseObjectBase::SetOption(key, value);
  }
};

class LogDriverConnection : public ConnectionObjectBase {
 public:
  LogDriverConnection() { Rprintf("LogConnectionNew()\n"); }

  ~LogDriverConnection() { Rprintf("LogConnectionRelease()\n"); }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    Rprintf("LogConnectionInit()\n");
    return ConnectionObjectBase::Init(parent, error);
  }

  const Option& GetOption(const std::string& key,
                          const Option& default_value = Option()) const {
    Rprintf("LogConnectionGetOption()\n");
    return ConnectionObjectBase::GetOption(key, default_value);
  }

  AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    Rprintf("LogConnectionSetOption()\n");
    return ConnectionObjectBase::SetOption(key, value);
  }

  AdbcStatusCode Commit(AdbcError* error) {
    Rprintf("LogConnectionCommit()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetInfo(const uint32_t* info_codes, size_t info_codes_length,
                         ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetInfo()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetObjects(int depth, const char* catalog, const char* db_schema,
                            const char* table_name, const char** table_type,
                            const char* column_name, ArrowArrayStream* out,
                            AdbcError* error) {
    Rprintf("LogConnectionGetObjects()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, ArrowSchema* schema,
                                AdbcError* error) {
    Rprintf("LogConnectionGetTableSchema()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetTableTypes(ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetTableTypes()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode ReadPartition(const uint8_t* serialized_partition,
                               size_t serialized_length, ArrowArrayStream* out,
                               AdbcError* error) {
    Rprintf("LogConnectionReadPartition()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Rollback(AdbcError* error) {
    Rprintf("LogConnectionRollback()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Cancel(AdbcError* error) {
    Rprintf("LogConnectionCancel()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetStatistics(const char* catalog, const char* db_schema,
                               const char* table_name, char approximate,
                               ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetStatistics()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetStatisticNames(ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetStatisticNames()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
};

class LogDriverStatement : public StatementObjectBase {
 public:
  ~LogDriverStatement() { Rprintf("LogStatementRelease()\n"); }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    Rprintf("LogStatementNew()\n");
    return StatementObjectBase::Init(parent, error);
  }

  const Option& GetOption(const std::string& key,
                          const Option& default_value = Option()) const {
    Rprintf("LogStatementGetOption()\n");
    return StatementObjectBase::GetOption(key, default_value);
  }

  AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    Rprintf("LogStatementSetOption()\n");
    return StatementObjectBase::SetOption(key, value);
  }

  AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                              AdbcError* error) {
    Rprintf("LogStatementExecuteQuery()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode ExecuteSchema(ArrowSchema* schema, AdbcError* error) {
    Rprintf("LogStatementExecuteSchema()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Prepare(AdbcError* error) {
    Rprintf("LogStatementPrepare()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode SetSqlQuery(const char* query, AdbcError* error) {
    Rprintf("LogStatementSetSqlQuery()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode SetSubstraitPlan(const uint8_t* plan, size_t length, AdbcError* error) {
    Rprintf("LogStatementSetSubstraitPlan()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Bind(ArrowArray* values, ArrowSchema* schema, AdbcError* error) {
    Rprintf("LogStatementBind()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    Rprintf("LogStatementBindStream()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Cancel(AdbcError* error) {
    Rprintf("LogStatementCancel()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
};

using LogDriver =
    adbc::common::Driver<LogDriverDatabase, LogDriverConnection, LogDriverStatement>;

static AdbcStatusCode LogDriverInitFunc(int version, void* raw_driver, AdbcError* error) {
  return LogDriver::Init(version, raw_driver, error);
}

extern "C" SEXP RAdbcLogDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)LogDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}
