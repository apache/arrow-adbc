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

#include "driver_base.h"

#include <adbc.h>

using adbc::r::Option;

using VoidDriver =
    adbc::r::Driver<adbc::r::DatabaseObjectBase, adbc::r::ConnectionObjectBase,
                    adbc::r::StatementObjectBase>;

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

class MonkeyDriverStatement : public adbc::r::StatementObjectBase {
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

  virtual AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                                      AdbcError* error) {
    if (stream != nullptr) {
      std::memcpy(stream, &stream_, sizeof(ArrowArrayStream));
      stream_.release = nullptr;
    }

    *rows_affected = -1;
    return ADBC_STATUS_OK;
  }

 private:
  ArrowArrayStream stream_;
};

using MonkeyDriver =
    adbc::r::Driver<adbc::r::DatabaseObjectBase, adbc::r::ConnectionObjectBase,
                    MonkeyDriverStatement>;

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

class LogDriverDatabase : public adbc::r::DatabaseObjectBase {
 public:
  LogDriverDatabase() { Rprintf("LogDatabaseNew()\n"); }

  ~LogDriverDatabase() { Rprintf("LogDatabaseRelease()\n"); }

  AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    Rprintf("LogDatabaseSetOption()\n");
    return adbc::r::DatabaseObjectBase::SetOption(key, value);
  }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    Rprintf("LogDatabaseInit()\n");
    return adbc::r::DatabaseObjectBase::Init(parent, error);
  }

  const Option& GetOption(const std::string& key,
                          const Option& default_value = Option()) const {
    Rprintf("LogDatabaseGetOption()\n");
    return adbc::r::DatabaseObjectBase::GetOption(key, default_value);
  }
};

class LogDriverConnection : public adbc::r::ConnectionObjectBase {
 public:
  LogDriverConnection() { Rprintf("LogConnectionNew()\n"); }

  ~LogDriverConnection() { Rprintf("LogConnectionRelease()\n"); }

  AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    Rprintf("LogConnectionSetOption()\n");
    return adbc::r::ConnectionObjectBase::SetOption(key, value);
  }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    Rprintf("LogConnectionInit()\n");
    return adbc::r::ConnectionObjectBase::Init(parent, error);
  }

  const Option& GetOption(const std::string& key,
                          const Option& default_value = Option()) const {
    Rprintf("LogConnectionGetOption()\n");
    return adbc::r::ConnectionObjectBase::GetOption(key, default_value);
  }
};

class LogDriverStatement : public adbc::r::StatementObjectBase {
 public:
  ~LogDriverStatement() { Rprintf("LogStatementRelease()\n"); }

  AdbcStatusCode Init(void* parent, AdbcError* error) {
    Rprintf("LogStatementNew()\n");
    return adbc::r::StatementObjectBase::Init(parent, error);
  }

  AdbcStatusCode SetOption(const std::string& key, const Option& value) {
    Rprintf("LogStatementSetOption()\n");
    return adbc::r::StatementObjectBase::SetOption(key, value);
  }

  const Option& GetOption(const std::string& key,
                          const Option& default_value = Option()) const {
    Rprintf("LogStatementGetOption()\n");
    return adbc::r::StatementObjectBase::GetOption(key, default_value);
  }

  AdbcStatusCode SetSqlQuery(const char* query, AdbcError* error) {
    Rprintf("LogStatementSetSqlQuery()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                              AdbcError* error) {
    Rprintf("LogStatementExecuteQuery()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    Rprintf("LogStatementBindStream()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
};

using LogDriver =
    adbc::r::Driver<LogDriverDatabase, LogDriverConnection, LogDriverStatement>;

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
