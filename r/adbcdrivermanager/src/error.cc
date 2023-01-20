#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <cstring>

#include <adbc.h>
#include "adbc_driver_manager.h"
#include "radbc.h"

static void finalize_error_xptr(SEXP error_xptr) {
  auto error = reinterpret_cast<AdbcError*>(R_ExternalPtrAddr(error_xptr));
  if (error != nullptr && error->release != nullptr) {
    error->release(error);
  }

  adbc_xptr_default_finalize<AdbcError>(error_xptr);
}

extern "C" SEXP RAdbcAllocateError(SEXP shelter_sexp) {
  SEXP error_xptr = PROTECT(adbc_allocate_xptr<AdbcError>(shelter_sexp));
  R_RegisterCFinalizer(error_xptr, &finalize_error_xptr);

  AdbcError* error = adbc_from_xptr<AdbcError>(error_xptr);
  error->message = nullptr;
  error->vendor_code = 0;
  memset(error->sqlstate, 0, sizeof(error->sqlstate));
  error->release = nullptr;

  UNPROTECT(1);
  return error_xptr;
}

extern "C" SEXP RAdbcErrorProxy(SEXP error_xptr) {
  AdbcError* error = adbc_from_xptr<AdbcError>(error_xptr);
  const char* names[] = {"message", "vendor_code", "sqlstate", ""};
  SEXP result = PROTECT(Rf_mkNamed(VECSXP, names));

  if (error->message != nullptr) {
    SEXP error_message = PROTECT(Rf_allocVector(STRSXP, 1));
    SET_STRING_ELT(error_message, 0, Rf_mkCharCE(error->message, CE_UTF8));
    SET_VECTOR_ELT(result, 0, error_message);
    UNPROTECT(1);
  }

  SET_VECTOR_ELT(result, 1, Rf_ScalarInteger(error->vendor_code));

  SEXP sqlstate = PROTECT(Rf_allocVector(RAWSXP, sizeof(error->sqlstate)));
  memcpy(RAW(sqlstate), error->sqlstate, sizeof(error->sqlstate));
  SET_VECTOR_ELT(result, 2, sqlstate);

  UNPROTECT(2);
  return result;
}

extern "C" SEXP RAdbcStatusCodeMessage(SEXP status_sexp) {
  int status = adbc_as_int(status_sexp);
  const char* msg = AdbcStatusCodeMessage(status);
  return Rf_mkString(msg);
}
