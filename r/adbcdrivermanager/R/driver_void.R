
#' Create ADBC drivers
#'
#' Creates the R object representation of an ADBC driver, which consists of a
#' name and an initializer function with an optional subclass to control
#' finer-grained behaviour at the R level.
#'
#' @param x,entrypoint An ADBC driver may be defined either as an init function
#'   or as an identifier with an entrypoint name. A driver init func
#'   must be an external pointer to a DL_FUNC with the type
#'   `AdbcDriverInitFunc` specified in the adbc.h header.
#' @param ... Further key/value parameters to store with the (R-level) driver
#'   object.
#' @param subclass An optional subclass for finer-grained control of
#'   behaviour at the R level.
#'
#' @return An object of class 'adbc_driver'
#' @export
#'
#' @examples
#' adbc_driver_void()
#'
adbc_driver_void <- function() {
  if (is.null(internal_driver_env$void)) {
    internal_driver_env$void <- adbc_driver(
      .Call(RAdbcVoidDriverInitFunc),
      subclass = "adbc_driver_void"
    )
  }

  internal_driver_env$void
}

#' @rdname adbc_driver_void
#' @export
adbc_driver <- function(x, entrypoint = NULL, ..., subclass = character()) {
  if (inherits(x, "adbc_driver_init_func")) {
    driver <- .Call(RAdbcLoadDriverFromInitFunc, x)
    driver$driver_init_func <- x
  } else {
    driver <- .Call(RAdbcLoadDriver, x, entrypoint)
    driver$name <- x
    driver$entrypoint <- entrypoint
  }

  args <- list(...)
  for (i in seq_along(args)) {
    driver[[names(args)[i]]] <- args[[i]]
  }

  class(driver) <- c(subclass, class(driver))
  driver
}

internal_driver_env <- new.env(parent = emptyenv())

#' @export
print.adbc_driver <- function(x, ...) {
  str(x, ...)
}

#' @importFrom utils str
#' @export
str.adbc_driver <- function(object, ...) {
  cat(sprintf("<%s> ", class(object)[1]))

  fields <- names(object)
  object_lst <- Map("[[", list(object), fields)
  names(object_lst) <- fields

  str(object_lst, ...)
  invisible(object)
}
