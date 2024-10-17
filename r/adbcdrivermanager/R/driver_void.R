# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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
#' @param driver An external pointer to an `AdbcDriver`
#' @param version The version number corresponding to the `driver` supplied
#' @param error An external pointer to an `AdbcError` or NULL
#'
#' @return
#' - `adbc_driver()`, `adbc_driver_void()`: An object of class 'adbc_driver'
#' - `adbc_driver_load_raw()`: An integer status code.
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
  error <- adbc_allocate_error()
  driver_alloc <- .Call(RAdbcAllocateDriver)
  driver <- driver_alloc$driver
  version <- driver_alloc$version

  # Attempt to load the driver to ensure it works
  status <- adbc_driver_load_raw(x, entrypoint, version, driver, error)
  stop_for_error(status, error)

  # Keep track of the version/mechanism used to successfully load a driver
  driver$version <- version
  if (inherits(x, "adbc_driver_init_func")) {
    driver$driver_init_func <- x
  } else {
    driver$name <- x
    driver$entrypoint <- entrypoint
  }

  # Add extra properties
  args <- list(...)
  for (i in seq_along(args)) {
    driver[[names(args)[i]]] <- args[[i]]
  }

  class(driver) <- c(subclass, class(driver))
  driver
}

#' @rdname adbc_driver_void
#' @export
adbc_driver_load_raw <- function(x, entrypoint, version, driver, error) {
  if (inherits(x, "adbc_driver_init_func")) {
    .Call(RAdbcLoadDriverFromInitFunc, x, version, driver, error)
  } else {
    .Call(RAdbcLoadDriver, x, entrypoint, version, driver, error)
  }
}

#' @rdname adbc_driver_void
#' @export
adbc_driver_resolve <- function(x, entrypoint = NULL) {
  if (!is.character(x)) {
    return(adbc_driver(x))
  }

  if (isTRUE(grepl("duckdb", x)) && identical(entrypoint, "duckdb_adbc_init")) {
    pkg <- "duckdb"
    fun <- "duckdb_adbc"
    asNamespace(pkg)[[fun]]()
  } else if (isTRUE(startsWith(x, "adbc_driver_")) && is.null(entrypoint)) {
    # adbc_driver_sqlite to adbcsqlite
    pkg <- gsub("^adbc_driver_", "adbc", x)
    fun <- pkg
    asNamespace(pkg)[[fun]]()
  } else {
    adbc_driver(x, entrypoint)
  }
}

#' @rdname adbc_driver_void
#' @export
adbc_driver_load <- function(x, entrypoint, version, driver, error) {
  driver <- adbc_driver_resolve(x, entrypoint)
  if (!is.null(driver$driver_init_func)) {
    adbc_driver_load_raw(driver$driver_init_func, NULL, version, driver, error)
  } else {
    adbc_driver_load_raw(driver$name, driver$entrypoint, NULL, version, driver, error)
  }
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
