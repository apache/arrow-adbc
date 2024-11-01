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

typedef int AdbcStatusCode;
struct AdbcError;
AdbcStatusCode AdbcDriverInit(int version, void* raw_driver, struct AdbcError* error);

static SEXP init_func_xptr = 0;

SEXP adbcbigquery_c_bigquery(void) { return init_func_xptr; }

static const R_CallMethodDef CallEntries[] = {
    {"adbcbigquery_c_bigquery", (DL_FUNC)&adbcbigquery_c_bigquery, 0}, {NULL, NULL, 0}};

void R_init_adbcbigquery(DllInfo* dll) {
  R_registerRoutines(dll, NULL, CallEntries, NULL, NULL);
  R_useDynamicSymbols(dll, FALSE);

  init_func_xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)AdbcDriverInit, R_NilValue, R_NilValue));
  Rf_setAttrib(init_func_xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  R_PreserveObject(init_func_xptr);
  UNPROTECT(1);
}
