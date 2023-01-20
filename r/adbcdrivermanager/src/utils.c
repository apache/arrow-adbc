#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

SEXP RAdbcXptrEnv(SEXP xptr) {
  if (TYPEOF(xptr) != EXTPTRSXP) {
    Rf_error("object is not an external pointer");
  }

  return R_ExternalPtrTag(xptr);
}
