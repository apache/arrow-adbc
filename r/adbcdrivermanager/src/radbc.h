
#pragma once

#include <R.h>
#include <Rinternals.h>

template <typename T>
static inline const char* adbc_xptr_class();

template <>
inline const char* adbc_xptr_class<AdbcError>() {
  return "adbc_error";
}

template <>
inline const char* adbc_xptr_class<AdbcDriver>() {
  return "adbc_driver";
}

template <>
inline const char* adbc_xptr_class<AdbcDatabase>() {
  return "adbc_database";
}

template <>
inline const char* adbc_xptr_class<AdbcConnection>() {
  return "adbc_connection";
}

template <>
inline const char* adbc_xptr_class<AdbcStatement>() {
  return "adbc_statement";
}

template <>
inline const char* adbc_xptr_class<ArrowArrayStream>() {
  return "nanoarrow_array_stream";
}

template <>
inline const char* adbc_xptr_class<ArrowArray>() {
  return "nanoarrow_array";
}

template <>
inline const char* adbc_xptr_class<ArrowSchema>() {
  return "nanoarrow_schema";
}

template <typename T>
static inline T* adbc_from_xptr(SEXP xptr) {
  if (!Rf_inherits(xptr, adbc_xptr_class<T>())) {
    Rf_error("Expected external pointer with class '%s'", adbc_xptr_class<T>());
  }

  T* ptr = reinterpret_cast<T*>(R_ExternalPtrAddr(xptr));
  if (ptr == nullptr) {
    Rf_error("Can't convert external pointer to NULL to T*");
  }
  return ptr;
}

template <typename T>
static inline SEXP adbc_allocate_xptr(SEXP shelter_sexp = R_NilValue) {
  void* ptr = malloc(sizeof(T));
  if (ptr == nullptr) {
    Rf_error("Failed to allocate T");
  }

  memset(ptr, 0, sizeof(T));
  SEXP xptr = PROTECT(R_MakeExternalPtr(ptr, R_NilValue, shelter_sexp));
  SEXP xptr_class = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(xptr_class, 0, Rf_mkChar(adbc_xptr_class<T>()));
  SET_STRING_ELT(xptr_class, 1, Rf_mkChar("adbc_xptr"));
  Rf_setAttrib(xptr, R_ClassSymbol, xptr_class);
  UNPROTECT(1);

  SEXP new_env_call = PROTECT(Rf_lang1(Rf_install("new_env")));
  SEXP new_env = PROTECT(Rf_eval(new_env_call, R_FindNamespace(Rf_mkString("adbcdrivermanager"))));
  R_SetExternalPtrTag(xptr, new_env);
  UNPROTECT(2);

  UNPROTECT(1);
  return xptr;
}

template <typename T>
static inline void adbc_xptr_default_finalize(SEXP xptr) {
  T* ptr = reinterpret_cast<T*>(R_ExternalPtrAddr(xptr));
  if (ptr != nullptr) {
    free(ptr);
  }
}

static inline const char* adbc_as_const_char(SEXP sexp) {
  if (TYPEOF(sexp) != STRSXP || Rf_length(sexp) != 1) {
    Rf_error("Expected character(1) for conversion to const char*");
  }

  SEXP item = STRING_ELT(sexp, 0);
  if (item == NA_STRING) {
    Rf_error("Can't convert NA_character_ to const char*");
  }

  return Rf_translateCharUTF8(item);
}

static inline int adbc_as_int(SEXP sexp) {
  if (Rf_length(sexp) == 1) {
    switch (TYPEOF(sexp)) {
      case REALSXP:
        return REAL(sexp)[0];
      case INTSXP:
        return INTEGER(sexp)[0];
    }
  }

  Rf_error("Expected integer(1) or double(1) for conversion to int");
}

static inline SEXP adbc_wrap_status(AdbcStatusCode code) {
  return Rf_ScalarInteger(code);
}
