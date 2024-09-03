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

#include "arrow-adbc/adbc.h"
#include "arrow-adbc/adbc_driver_manager.h"
#include "radbc.h"

static void finalize_error_xptr(SEXP error_xptr) {
  auto error = reinterpret_cast<AdbcError*>(R_ExternalPtrAddr(error_xptr));
  if (error != nullptr && error->release != nullptr) {
    error->release(error);
  }

  adbc_xptr_default_finalize<AdbcError>(error_xptr);
}

extern "C" SEXP RAdbcAllocateError(SEXP shelter_sexp, SEXP use_legacy_error_sexp) {
  bool use_legacy_error = adbc_as_bool(use_legacy_error_sexp);

  SEXP error_xptr = PROTECT(adbc_allocate_xptr<AdbcError>(shelter_sexp));
  R_RegisterCFinalizer(error_xptr, &finalize_error_xptr);

  AdbcError* error = adbc_from_xptr<AdbcError>(error_xptr);
  *error = ADBC_ERROR_INIT;
  if (use_legacy_error) {
    error->vendor_code = 0;
  }

  UNPROTECT(1);
  return error_xptr;
}

static SEXP wrap_error_details(AdbcError* error) {
  int n_details = AdbcErrorGetDetailCount(error);
  SEXP result_names = PROTECT(Rf_allocVector(STRSXP, n_details));
  SEXP result = PROTECT(Rf_allocVector(VECSXP, n_details));

  for (int i = 0; i < n_details; i++) {
    AdbcErrorDetail item = AdbcErrorGetDetail(error, i);
    SET_STRING_ELT(result_names, i, Rf_mkCharCE(item.key, CE_UTF8));
    SEXP item_sexp = PROTECT(Rf_allocVector(RAWSXP, item.value_length));
    memcpy(RAW(item_sexp), item.value, item.value_length);
    SET_VECTOR_ELT(result, i, item_sexp);
    UNPROTECT(1);
  }

  Rf_setAttrib(result, R_NamesSymbol, result_names);
  UNPROTECT(2);
  return result;
}

extern "C" SEXP RAdbcErrorProxy(SEXP error_xptr) {
  AdbcError* error = adbc_from_xptr<AdbcError>(error_xptr);
  const char* names[] = {"message", "vendor_code", "sqlstate", "details", ""};
  SEXP result = PROTECT(Rf_mkNamed(VECSXP, names));

  if (error->message != nullptr) {
    SEXP error_message = PROTECT(Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(error_message, 0, Rf_mkCharCE(error->message, CE_UTF8));
    SET_VECTOR_ELT(result, 0, error_message);
    UNPROTECT(1);
  }

  SEXP vendor_code = PROTECT(Rf_ScalarInteger(error->vendor_code));
  SET_VECTOR_ELT(result, 1, vendor_code);
  UNPROTECT(1);

  SEXP sqlstate = PROTECT(Rf_allocVector(RAWSXP, sizeof(error->sqlstate)));
  memcpy(RAW(sqlstate), error->sqlstate, sizeof(error->sqlstate));
  SET_VECTOR_ELT(result, 2, sqlstate);
  UNPROTECT(1);

  SEXP details = PROTECT(wrap_error_details(error));
  SET_VECTOR_ELT(result, 3, details);
  UNPROTECT(1);

  UNPROTECT(1);
  return result;
}

extern "C" SEXP RAdbcErrorFromArrayStream(SEXP stream_xptr) {
  struct ArrowArrayStream* stream =
      reinterpret_cast<ArrowArrayStream*>(R_ExternalPtrAddr(stream_xptr));

  AdbcStatusCode status = ADBC_STATUS_OK;
  const AdbcError* error = AdbcErrorFromArrayStream(stream, &status);
  if (error == nullptr) {
    return R_NilValue;
  }

  // Not using a normal error_xptr here because the lifecycle is managed by the stream.
  // This logic won't survive accesses to the error following an explicit stream release;
  // however will at least keep a stream from being released via the garbage collector.
  SEXP error_xptr =
      PROTECT(adbc_borrow_xptr<AdbcError>(const_cast<AdbcError*>(error), stream_xptr));

  SEXP status_sexp = PROTECT(adbc_wrap_status(status));

  SEXP result = PROTECT(Rf_allocVector(VECSXP, 2));
  SET_VECTOR_ELT(result, 0, status_sexp);
  SET_VECTOR_ELT(result, 1, error_xptr);
  UNPROTECT(3);
  return result;
}

extern "C" SEXP RAdbcStatusCodeMessage(SEXP status_sexp) {
  int status = adbc_as_int(status_sexp);
  const char* msg = AdbcStatusCodeMessage(status);
  return Rf_mkString(msg);
}
