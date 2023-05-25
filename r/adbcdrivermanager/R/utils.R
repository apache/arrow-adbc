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

key_value_options <- function(options) {
  if (!is.character(options)) {
    options <- as.list(options)
    options <- options[!vapply(options, is.null, logical(1))]
    options <- vapply(options, as.character, character(1))
  }

  keys <- names(options)
  if (length(options) == 0) {
    names(options) <- character()
  } else if (is.null(keys) || all(keys == "")) {
    stop("key/value options must be named")
  }

  options
}

new_env <- function() {
  new.env(parent = emptyenv())
}

xptr_env <- function(xptr) {
  .Call(RAdbcXptrEnv, xptr)
}

#' @export
length.adbc_xptr <- function(x) {
  length(xptr_env(x))
}

#' @export
names.adbc_xptr <- function(x) {
  names(xptr_env(x))
}


#' @export
`[[.adbc_xptr` <- function(x, i) {
  xptr_env(x)[[i]]
}

#' @export
`[[<-.adbc_xptr` <- function(x, i, value) {
  env <- xptr_env(x)
  env[[i]] <- value
  invisible(x)
}

#' @export
`$.adbc_xptr` <- function(x, name) {
  xptr_env(x)[[name]]
}

#' @export
`$<-.adbc_xptr` <- function(x, name, value) {
  env <- xptr_env(x)
  env[[name]] <- value
  invisible(x)
}

#' @export
print.adbc_xptr <- function(x, ...) {
  str(x, ...)
}

#' @export
str.adbc_xptr <- function(object, ...) {
  cat(sprintf("<%s> %s ", class(object)[1], format(object)))
  env_proxy <- as.list(xptr_env(object))
  env_proxy$options <- as.list(env_proxy$options)
  str(env_proxy, ...)
  invisible(object)
}


#' Low-level pointer details
#'
#' - `adbc_xptr_move()` allocates a fresh R object and moves all values pointed
#'   to by `x` into it. The original R object is invalidated by zeroing its
#'   content. This is useful when returning from a function where
#'   [lifecycle helpers][with_adbc] were used to manage the original
#'   object.
#' - `adbc_xptr_is_valid()` provides a means by which to test for an invalidated
#'   pointer.
#'
#' @param x An 'adbc_database', 'adbc_connection', 'adbc_statement', or
#'   'nanoarrow_array_stream'
#'
#' @return
#' - `adbc_xptr_move()`: A freshly-allocated R object identical to `x`
#' - `adbc_xptr_is_valid()`: Returns FALSE if the ADBC object pointed to by `x`
#'   has been invalidated.
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_void())
#' adbc_xptr_is_valid(db)
#' db_new <- adbc_xptr_move(db)
#' adbc_xptr_is_valid(db)
#' adbc_xptr_is_valid(db_new)
#'
adbc_xptr_move <- function(x) {
  if (inherits(x, "adbc_database")) {
    .Call(RAdbcMoveDatabase, x)
  } else if (inherits(x, "adbc_connection")) {
    .Call(RAdbcMoveConnection, x)
  } else if (inherits(x, "adbc_statement")) {
    .Call(RAdbcMoveStatement, x)
  } else if (inherits(x, "nanoarrow_array_stream")) {
    stream <- nanoarrow::nanoarrow_allocate_array_stream()
    nanoarrow::nanoarrow_pointer_move(x, stream)
    stream
  } else {
    assert_adbc(x)
  }
}

#' @rdname adbc_xptr_move
#' @export
adbc_xptr_is_valid <- function(x) {
  if (inherits(x, "adbc_database")) {
    .Call(RAdbcDatabaseValid, x)
  } else if (inherits(x, "adbc_connection")) {
    .Call(RAdbcConnectionValid, x)
  } else if (inherits(x, "adbc_statement")) {
    .Call(RAdbcStatementValid, x)
  } else if (inherits(x, "nanoarrow_array_stream")) {
    nanoarrow::nanoarrow_pointer_is_valid(x)
  } else {
    assert_adbc(x)
  }
}

# Usually we want errors for an attempt at double release; however,
# the helpers we want to be compatible with adbc_xptr_move() which sets the
# managed pointer to NULL.
adbc_release_non_null <- function(x) {
  if (!adbc_xptr_is_valid(x)) {
    return()
  }

  if (inherits(x, "adbc_database")) {
    adbc_database_release(x)
  } else if (inherits(x, "adbc_connection")) {
    adbc_connection_release(x)
  } else if (inherits(x, "adbc_statement")) {
    adbc_statement_release(x)
  } else if (inherits(x, "nanoarrow_array_stream")) {
    nanoarrow::nanoarrow_pointer_release(x)
  } else {
    assert_adbc(x)
  }
}

adbc_classes <- c(
  "adbc_database", "adbc_connection", "adbc_statement",
  "nanoarrow_array_stream"
)

assert_adbc <- function(x, what = adbc_classes) {
  if (inherits(x, what)) {
    return(invisible(x))
  }

  stop(
    sprintf(
      "`x` must inherit from one of: %s",
      paste0("'", what, "'", collapse = ", ")
    ),
    call. = sys.call(-1)
  )
}
