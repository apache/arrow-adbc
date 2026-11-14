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


#' Get extended error information from an array stream
#'
#' @param stream A [nanoarrow_array_stream][nanoarrow::as_nanoarrow_array_stream]
#'
#' @return `NULL` if stream was not created by a driver that supports
#'   extended error information or a list whose first element is the
#'   status code and second element is the `adbc_error` object. The
#'   `acbc_error` must not be accessed if `stream` is explicitly released.
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_monkey())
#' con <- adbc_connection_init(db)
#' stmt <- adbc_statement_init(con, mtcars)
#' stream <- nanoarrow::nanoarrow_allocate_array_stream()
#' adbc_statement_execute_query(stmt, stream)
#' adbc_error_from_array_stream(stream)
#'
adbc_error_from_array_stream <- function(stream) {
  if (!inherits(stream, "nanoarrow_array_stream") || !adbc_xptr_is_valid(stream)) {
    stop("`stream` must be a valid nanoarrow_array_stream")
  }

  .Call(RAdbcErrorFromArrayStream, stream)
}

adbc_allocate_error <- function(shelter = NULL, use_legacy_error = NULL) {
  if (is.null(use_legacy_error)) {
    use_legacy_error <- getOption("adbcdrivermanager.use_legacy_error", FALSE)
  }

  .Call(RAdbcAllocateError, shelter, use_legacy_error)
}

stop_for_error <- function(status, error) {
  if (!identical(status, 0L)) {
    if (inherits(error, "adbc_error")) {
      error <- .Call(RAdbcErrorProxy, error)
    } else {
      error <- list()
    }

    error$status <- status
    error$status_code_message <- .Call(RAdbcStatusCodeMessage, status)
    if (!is.null(error$message)) {
      msg <- paste(error$status_code_message, error$message, sep=": ")
    } else {
      msg <- error$status_code_message
    }

    # Gives an error class like "adbc_status_invalid_state", "adbc_status",
    # "simpleError", ...
    cnd_class <- c(
      paste0("adbc_status_", tolower(gsub("\\s+.*", "", error$status_code_message))),
      "adbc_status"
    )

    # Strips stop_for_error() call from the error output
    cnd <- simpleError(msg, call = sys.call(-1))
    class(cnd) <- union(cnd_class, class(cnd))

    # Store the listified error information
    cnd$error <- error

    stop(cnd)
  }
}

adbc_error_message <- function(status, error) {
  if (!identical(status, 0L)) {
    if (inherits(error, "adbc_error")) {
      error <- .Call(RAdbcErrorProxy, error)
    } else {
      error <- list()
    }

    error$status <- status
    error$status_code_message <- .Call(RAdbcStatusCodeMessage, status)
    if (!is.null(error$message)) error$message else error$status_code_message
  } else {
    "OK"
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
  4L
}

#' @export
names.adbc_error <- function(x, ...) {
  c("message", "vendor_code", "sqlstate", "details")
}

#' @export
`[[.adbc_error` <- function(x, i, ...) {
  .Call(RAdbcErrorProxy, x)[[i]]
}

#' @export
`$.adbc_error` <- function(x, i, ...) {
  x[[i]]
}
