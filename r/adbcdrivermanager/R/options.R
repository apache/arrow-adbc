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

#' @rdname adbc_database_init
#' @export
adbc_database_set_options <- function(database, options) {
  options <- key_value_options(options)
  error <- adbc_allocate_error()
  for (i in seq_along(options)) {
    key <- names(options)[i]
    value <- options[[i]]
    status <- .Call(
      RAdbcDatabaseSetOption,
      database,
      key,
      value,
      error
    )
    stop_for_error(status, error)
  }
  invisible(database)
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_set_options <- function(connection, options) {
  options <- key_value_options(options)
  error <- adbc_allocate_error()
  for (i in seq_along(options)) {
    key <- names(options)[i]
    value <- options[[i]]
    status <- .Call(
      RAdbcConnectionSetOption,
      connection,
      key,
      value,
      error
    )
    stop_for_error(status, error)
  }
  invisible(connection)
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_set_options <- function(statement, options) {
  options <- key_value_options(options)
  error <- adbc_allocate_error()
  for (i in seq_along(options)) {
    key <- names(options)[i]
    value <- options[[i]]
    status <- .Call(
      RAdbcStatementSetOption,
      statement,
      key,
      value,
      error
    )
    stop_for_error(status, error)
  }
  invisible(statement)
}

#' @rdname adbc_database_init
#' @export
adbc_database_get_option <- function(database, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcDatabaseGetOption, database, option, error)
}

#' @rdname adbc_database_init
#' @export
adbc_database_get_option_bytes <- function(database, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcDatabaseGetOptionBytes, database, option, error)
}

#' @rdname adbc_database_init
#' @export
adbc_database_get_option_int <- function(database, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcDatabaseGetOptionInt, database, option, error)
}

#' @rdname adbc_database_init
#' @export
adbc_database_get_option_double <- function(database, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcDatabaseGetOptionDouble, database, option, error)
}


#' @rdname adbc_connection_init
#' @export
adbc_connection_get_option <- function(connection, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcConnectionGetOption, connection, option, error)
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_get_option_bytes <- function(connection, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcConnectionGetOptionBytes, connection, option, error)
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_get_option_int <- function(connection, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcConnectionGetOptionInt, connection, option, error)
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_get_option_double <- function(connection, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcConnectionGetOptionDouble, connection, option, error)
}


#' @rdname adbc_statement_init
#' @export
adbc_statement_get_option <- function(statement, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcStatementGetOption, statement, option, error)
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_get_option_bytes <- function(statement, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcStatementGetOptionBytes, statement, option, error)
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_get_option_int <- function(statement, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcStatementGetOptionInt, statement, option, error)
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_get_option_double <- function(statement, option) {
  error <- adbc_allocate_error()
  .Call(RAdbcStatementGetOptionDouble, statement, option, error)
}

# Ensures that options are a list of bare character, raw, integer, or double
key_value_options <- function(options) {
  options <- as.list(options)

  if (length(options) == 0) {
    names(options) <- character()
  } else if (is.null(names(options))) {
    # OK to have no names, because options could contain a series of
    # adbc_options() objects that will be concatenated
    names(options) <- rep("", length(options))
  }

  out <- vector("list", 10L)
  out_names <- character(10L)
  n_out <- 0L

  for (i in seq_along(options)) {
    key <- names(options)[[i]]
    item <- options[[i]]

    # Skip NULL item
    if (is.null(item)) {
      next
    }

    # Append all items of an existing adbc_options
    if (inherits(item, "adbc_options")) {
      out <- c(out[seq_len(n_out)], item)
      out_names <- c(out_names[seq_len(n_out)], names(item))
      n_out <- n_out + length(item)
      next
    }

    # Otherwise, append a single value (coercing to character if item
    # is an S3 object)
    if (is.object(item)) {
      item <- as.character(item)
    }

    if (identical(key, "") || identical(key, NA_character_)) {
      stop("key/value options must be named")
    }

    n_out <- n_out + 1L
    out_names[n_out] <- key
    if (is.character(item)) {
      out[[n_out]] <- item
    } else if (is.integer(item)) {
      out[[n_out]] <- as.integer(item)
    } else if (is.double(item)) {
      out[[n_out]] <- as.double(item)
    } else if (is.raw(item)) {
      out[[n_out]] <- item
    } else if (is.logical(item)) {
      out[[n_out]] <- tolower(as.character(item))
    } else {
      stop(
        sprintf(
          "Option of type '%s' (key: '%s') not supported",
           typeof(item),
          key
        )
      )
    }
  }

  names(out) <- out_names
  structure(out[seq_len(n_out)], class = "adbc_options")
}
