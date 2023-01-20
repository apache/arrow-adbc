
adbc_allocate_error <- function(shelter = NULL) {
  .Call(RAdbcAllocateError, shelter)
}

stop_for_error <- function(status, error) {
  if (!identical(status, 0L)) {
    msg <- if (!is.null(error$message)) error$message else .Call(RAdbcStatusCodeMessage, status)
    stop(msg)
  }
}

#' @export
print.adbc_error <- function(x, ...) {
  str(x, ...)
}

#' @importFrom utils str
#' @export
str.adbc_error <- function(object, ...) {
  cat("<adbc_error> ")
  str(.Call(RAdbcErrorProxy, object), ...)
  invisible(object)
}

# This is the list()-like interface that allows $ and [[
# to make nice auto-complete when interacting in an IDE

#' @export
length.adbc_error <- function(x, ...) {
  3L
}

#' @export
names.adbc_error <- function(x, ...) {
  c("message", "vendor_code", "sqlstate")
}

#' @export
`[[.adbc_error` <- function(x, i, ...) {
  .Call(RAdbcErrorProxy, x)[[i]]
}

#' @export
`$.adbc_error` <- function(x, i, ...) {
  x[[i]]
}

