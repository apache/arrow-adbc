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

#include "arrow-adbc/adbc.h"

#include "radbc.h"

template <typename T>
static inline T adbc_as_c(SEXP sexp);

template <>
inline const char* adbc_as_c(SEXP sexp) {
  return adbc_as_const_char(sexp);
}

template <>
inline int64_t adbc_as_c(SEXP sexp) {
  return adbc_as_int64(sexp);
}

template <>
inline double adbc_as_c(SEXP sexp) {
  return adbc_as_double(sexp);
}

template <typename T, typename ValueT>
SEXP adbc_set_option(SEXP obj_xptr, SEXP key_sexp, SEXP value_sexp, SEXP error_xptr,
                     AdbcStatusCode (*SetOption)(T*, const char*, ValueT, AdbcError*)) {
  auto obj = adbc_from_xptr<T>(obj_xptr);
  const char* key = adbc_as_const_char(key_sexp);
  ValueT value = adbc_as_c<ValueT>(value_sexp);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);
  return adbc_wrap_status(SetOption(obj, key, value, error));
}

template <typename T>
SEXP adbc_set_option_bytes(SEXP obj_xptr, SEXP key_sexp, SEXP value_sexp, SEXP error_xptr,
                           AdbcStatusCode (*SetOption)(T*, const char*, const uint8_t*,
                                                       size_t, AdbcError*)) {
  auto obj = adbc_from_xptr<T>(obj_xptr);
  const char* key = adbc_as_const_char(key_sexp);
  const uint8_t* value = RAW(value_sexp);
  size_t value_length = Rf_xlength(value_sexp);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  int status = SetOption(obj, key, value, value_length, error);
  return adbc_wrap_status(status);
}

extern "C" SEXP RAdbcDatabaseSetOption(SEXP database_xptr, SEXP key_sexp, SEXP value_sexp,
                                       SEXP error_xptr) {
  switch (TYPEOF(value_sexp)) {
    case STRSXP:
      return adbc_set_option<AdbcDatabase, const char*>(
          database_xptr, key_sexp, value_sexp, error_xptr, &AdbcDatabaseSetOption);
    case RAWSXP:
      return adbc_set_option_bytes<AdbcDatabase>(database_xptr, key_sexp, value_sexp,
                                                 error_xptr, &AdbcDatabaseSetOptionBytes);
    case INTSXP:
      return adbc_set_option<AdbcDatabase, int64_t>(
          database_xptr, key_sexp, value_sexp, error_xptr, &AdbcDatabaseSetOptionInt);
    case REALSXP:
      return adbc_set_option<AdbcDatabase, double>(
          database_xptr, key_sexp, value_sexp, error_xptr, &AdbcDatabaseSetOptionDouble);
    default:
      Rf_error("Option value type not suppported");
  }
}

extern "C" SEXP RAdbcConnectionSetOption(SEXP connection_xptr, SEXP key_sexp,
                                         SEXP value_sexp, SEXP error_xptr) {
  switch (TYPEOF(value_sexp)) {
    case STRSXP:
      return adbc_set_option<AdbcConnection, const char*>(
          connection_xptr, key_sexp, value_sexp, error_xptr, &AdbcConnectionSetOption);
    case RAWSXP:
      return adbc_set_option_bytes<AdbcConnection>(connection_xptr, key_sexp, value_sexp,
                                                   error_xptr,
                                                   &AdbcConnectionSetOptionBytes);
    case INTSXP:
      return adbc_set_option<AdbcConnection, int64_t>(
          connection_xptr, key_sexp, value_sexp, error_xptr, &AdbcConnectionSetOptionInt);
    case REALSXP:
      return adbc_set_option<AdbcConnection, double>(connection_xptr, key_sexp,
                                                     value_sexp, error_xptr,
                                                     &AdbcConnectionSetOptionDouble);
    default:
      Rf_error("Option value type not suppported");
  }
}

extern "C" SEXP RAdbcStatementSetOption(SEXP statement_xptr, SEXP key_sexp,
                                        SEXP value_sexp, SEXP error_xptr) {
  switch (TYPEOF(value_sexp)) {
    case STRSXP:
      return adbc_set_option<AdbcStatement, const char*>(
          statement_xptr, key_sexp, value_sexp, error_xptr, &AdbcStatementSetOption);
    case RAWSXP:
      return adbc_set_option_bytes<AdbcStatement>(
          statement_xptr, key_sexp, value_sexp, error_xptr, &AdbcStatementSetOptionBytes);
    case INTSXP:
      return adbc_set_option<AdbcStatement, int64_t>(
          statement_xptr, key_sexp, value_sexp, error_xptr, &AdbcStatementSetOptionInt);
    case REALSXP:
      return adbc_set_option<AdbcStatement, double>(statement_xptr, key_sexp, value_sexp,
                                                    error_xptr,
                                                    &AdbcStatementSetOptionDouble);
    default:
      Rf_error("Option value type not suppported");
  }
}

template <typename T, typename CharT>
static inline SEXP adbc_get_option_bytes(SEXP obj_xptr, SEXP key_sexp, SEXP error_xptr,
                                         AdbcStatusCode (*GetOption)(T*, const char*,
                                                                     CharT*, size_t*,
                                                                     AdbcError*)) {
  auto obj = adbc_from_xptr<T>(obj_xptr);
  const char* key = adbc_as_const_char(key_sexp);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  size_t length = 0;
  int status = GetOption(obj, key, nullptr, &length, error);
  adbc_error_stop(status, error);

  SEXP result_shelter = PROTECT(Rf_allocVector(RAWSXP, length));
  auto result = reinterpret_cast<CharT*>(RAW(result_shelter));
  status = GetOption(obj, key, result, &length, error);
  adbc_error_stop(status, error);

  UNPROTECT(1);
  return result_shelter;
}

template <typename T>
static inline SEXP adbc_get_option(SEXP obj_xptr, SEXP key_sexp, SEXP error_xptr,
                                   AdbcStatusCode (*GetOption)(T*, const char*, char*,
                                                               size_t*, AdbcError*)) {
  SEXP bytes_sexp =
      adbc_get_option_bytes<T, char>(obj_xptr, key_sexp, error_xptr, GetOption);
  PROTECT(bytes_sexp);

  char* result = reinterpret_cast<char*>(RAW(bytes_sexp));
  SEXP result_char = PROTECT(Rf_mkCharLenCE(result, Rf_length(bytes_sexp) - 1, CE_UTF8));
  SEXP result_string = PROTECT(Rf_ScalarString(result_char));
  UNPROTECT(3);
  return result_string;
}

static inline SEXP adbc_wrap(int64_t value) {
  if (value <= NA_INTEGER || value >= INT_MAX) {
    return Rf_ScalarReal(value);
  } else {
    return Rf_ScalarInteger(value);
  }
}

static inline SEXP adbc_wrap(double value) { return Rf_ScalarReal(value); }

template <typename T, typename ResultT>
static inline SEXP adbc_get_option_numeric(SEXP obj_xptr, SEXP key_sexp, SEXP error_xptr,
                                           AdbcStatusCode (*GetOption)(T*, const char*,
                                                                       ResultT*,
                                                                       AdbcError*)) {
  auto obj = adbc_from_xptr<T>(obj_xptr);
  const char* key = adbc_as_const_char(key_sexp);
  auto error = adbc_from_xptr<AdbcError>(error_xptr);

  ResultT value = 0;
  int status = GetOption(obj, key, &value, error);
  adbc_error_stop(status, error);
  return adbc_wrap(value);
}

extern "C" SEXP RAdbcDatabaseGetOption(SEXP database_xptr, SEXP key_sexp,
                                       SEXP error_xptr) {
  return adbc_get_option<AdbcDatabase>(database_xptr, key_sexp, error_xptr,
                                       &AdbcDatabaseGetOption);
}

extern "C" SEXP RAdbcDatabaseGetOptionBytes(SEXP database_xptr, SEXP key_sexp,
                                            SEXP error_xptr) {
  return adbc_get_option_bytes<AdbcDatabase, uint8_t>(database_xptr, key_sexp, error_xptr,
                                                      &AdbcDatabaseGetOptionBytes);
}

extern "C" SEXP RAdbcDatabaseGetOptionInt(SEXP database_xptr, SEXP key_sexp,
                                          SEXP error_xptr) {
  return adbc_get_option_numeric<AdbcDatabase, int64_t>(
      database_xptr, key_sexp, error_xptr, &AdbcDatabaseGetOptionInt);
}

extern "C" SEXP RAdbcDatabaseGetOptionDouble(SEXP database_xptr, SEXP key_sexp,
                                             SEXP error_xptr) {
  return adbc_get_option_numeric<AdbcDatabase, double>(
      database_xptr, key_sexp, error_xptr, &AdbcDatabaseGetOptionDouble);
}

extern "C" SEXP RAdbcConnectionGetOption(SEXP connection_xptr, SEXP key_sexp,
                                         SEXP error_xptr) {
  return adbc_get_option<AdbcConnection>(connection_xptr, key_sexp, error_xptr,
                                         &AdbcConnectionGetOption);
}

extern "C" SEXP RAdbcConnectionGetOptionBytes(SEXP connection_xptr, SEXP key_sexp,
                                              SEXP error_xptr) {
  return adbc_get_option_bytes<AdbcConnection, uint8_t>(
      connection_xptr, key_sexp, error_xptr, &AdbcConnectionGetOptionBytes);
}

extern "C" SEXP RAdbcConnectionGetOptionInt(SEXP connection_xptr, SEXP key_sexp,
                                            SEXP error_xptr) {
  return adbc_get_option_numeric<AdbcConnection, int64_t>(
      connection_xptr, key_sexp, error_xptr, &AdbcConnectionGetOptionInt);
}

extern "C" SEXP RAdbcConnectionGetOptionDouble(SEXP connection_xptr, SEXP key_sexp,
                                               SEXP error_xptr) {
  return adbc_get_option_numeric<AdbcConnection, double>(
      connection_xptr, key_sexp, error_xptr, &AdbcConnectionGetOptionDouble);
}

extern "C" SEXP RAdbcStatementGetOption(SEXP statement_xptr, SEXP key_sexp,
                                        SEXP error_xptr) {
  return adbc_get_option<AdbcStatement>(statement_xptr, key_sexp, error_xptr,
                                        &AdbcStatementGetOption);
}

extern "C" SEXP RAdbcStatementGetOptionBytes(SEXP statement_xptr, SEXP key_sexp,
                                             SEXP error_xptr) {
  return adbc_get_option_bytes<AdbcStatement, uint8_t>(
      statement_xptr, key_sexp, error_xptr, &AdbcStatementGetOptionBytes);
}

extern "C" SEXP RAdbcStatementGetOptionInt(SEXP statement_xptr, SEXP key_sexp,
                                           SEXP error_xptr) {
  return adbc_get_option_numeric<AdbcStatement, int64_t>(
      statement_xptr, key_sexp, error_xptr, &AdbcStatementGetOptionInt);
}

extern "C" SEXP RAdbcStatementGetOptionDouble(SEXP statement_xptr, SEXP key_sexp,
                                              SEXP error_xptr) {
  return adbc_get_option_numeric<AdbcStatement, double>(
      statement_xptr, key_sexp, error_xptr, &AdbcStatementGetOptionDouble);
}
