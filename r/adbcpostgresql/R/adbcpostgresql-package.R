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

#' @keywords internal
#' @aliases NULL
"_PACKAGE"

## usethis namespace: start
#' @useDynLib adbcpostgresql, .registration = TRUE
## usethis namespace: end
NULL

#' ADBC SQLite3 Driver
#'
#' @inheritParams adbcdrivermanager::adbc_database_init
#' @inheritParams adbcdrivermanager::adbc_connection_init
#' @inheritParams adbcdrivermanager::adbc_statement_init
#' @param uri A URI to a database path or ":memory:" for an in-memory database.
#' @param adbc.connection.autocommit Use FALSE to disable the default
#'   autocommit behaviour.
#' @param adbc.ingest.target_table The name of the target table for a bulk insert.
#' @param adbc.ingest.mode Whether to create (the default) or append.
#' @param adbc.sqlite.query.batch_rows The number of rows per batch to return.
#'
#' @return An [adbcdrivermanager::adbc_driver()]
#' @export
#'
#' @examples
#' adbcpostgresql()
#'
adbcpostgresql <- function() {
  adbcdrivermanager::adbc_driver(
    .Call(adbcpostgresql_c_postgresql),
    subclass = "adbcpostgresql_driver_postgresql"
  )
}

#' @rdname adbcpostgresql
#' @importFrom adbcdrivermanager adbc_database_init
#' @export
adbc_database_init.adbcpostgresql_driver_postgresql <- function(driver, uri) {
  adbcdrivermanager::adbc_database_init_default(
    driver,
    list(uri = uri),
    subclass = "adbcpostgresql_database"
  )
}

#' @rdname adbcpostgresql
#' @importFrom adbcdrivermanager adbc_connection_init
#' @export
adbc_connection_init.adbcpostgresql_database <- function(database,
                                                     adbc.connection.autocommit = NULL) {
  options <- list(adbc.connection.autocommit = adbc.connection.autocommit)
  adbcdrivermanager::adbc_connection_init_default(
    database,
    options[!vapply(options, is.null, logical(1))],
    subclass = "adbcpostgresql_connection"
  )
}

#' @rdname adbcpostgresql
#' @importFrom adbcdrivermanager adbc_statement_init
#' @export
adbc_statement_init.adbcpostgresql_connection <- function(connection,
                                                      adbc.ingest.target_table = NULL,
                                                      adbc.ingest.mode = NULL,
                                                      adbc.sqlite.query.batch_rows = NULL) {
  options <- list(
    adbc.ingest.target_table = adbc.ingest.target_table,
    adbc.ingest.mode = adbc.ingest.mode,
    adbc.sqlite.query.batch_rows = adbc.sqlite.query.batch_rows
  )

  adbcdrivermanager::adbc_statement_init_default(
    connection,
    options[!vapply(options, is.null, logical(1))],
    subclass = "adbcpostgresql_statement"
  )
}

test_server_start <- function() {
  existing_test_uri <- Sys.getenv("ADBC_POSTGRESQL_TEST_URI", "")
  if (!identical(existing_test_uri, "")) {
    return(list(uri = existing_test_uri, process = NULL))
  }

  process <- processx::process$new(
    "docker",
    c(
      "run", "--rm", "-i",
      "-e", "POSTGRES_PASSWORD=password",
      "-e", "POSTGRES_DB=tempdb",
      "-p", "5432:5432",
      "postgres"
    ),
    stdout = "|",
    stderr = "|",
    cleanup = TRUE,
    cleanup_tree = TRUE
  )

  list(
    uri = "postgresql://localhost:5432/postgres?user=postgres&password=password",
    process = process
  )
}

test_server_print_stdout <- function(x) {
  if (!is.null(x$process)) {
    cat(x$process$read_output())
    cat("\n")
  }
}

test_server_print_stderr <- function(x) {
  if (!is.null(x$process)) {
    cat(x$process$read_error())
    cat("\n")
  }
}

test_server_stop <- function(x, timeout = 1) {
  if (!is.null(x$process)) {
    x$process$interrupt()
    x$process$wait(timeout)
  }
}
