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
