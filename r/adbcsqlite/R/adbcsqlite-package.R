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
#' @aliases adbcsqlite-package
"_PACKAGE"

## usethis namespace: start
#' @useDynLib adbcsqlite, .registration = TRUE
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
#' @param adbc.ingest.target_catalog The catalog of the table for a bulk insert.
#' @param adbc.ingest.mode Whether to create (the default) or append.
#' @param adbc.sqlite.query.batch_rows The number of rows per batch to return.
#'
#' @return An [adbcdrivermanager::adbc_driver()]
#' @export
#'
#' @examples
#' adbcsqlite()
#'
adbcsqlite <- function() {
  adbcdrivermanager::adbc_driver(
    .Call(adbcsqlite_c_sqlite),
    subclass = "adbcsqlite_driver_sqlite"
  )
}

#' @rdname adbcsqlite
#' @importFrom adbcdrivermanager adbc_database_init
#' @export
adbc_database_init.adbcsqlite_driver_sqlite <- function(driver, ..., uri = ":memory:") {
  adbcdrivermanager::adbc_database_init_default(
    driver,
    list(..., uri = uri),
    subclass = "adbcsqlite_database"
  )
}

#' @rdname adbcsqlite
#' @importFrom adbcdrivermanager adbc_connection_init
#' @export
adbc_connection_init.adbcsqlite_database <- function(database, ...,
                                                     adbc.connection.autocommit = NULL) {
  options <- list(..., adbc.connection.autocommit = adbc.connection.autocommit)
  adbcdrivermanager::adbc_connection_init_default(
    database,
    options,
    subclass = "adbcsqlite_connection"
  )
}

#' @rdname adbcsqlite
#' @importFrom adbcdrivermanager adbc_statement_init
#' @export
adbc_statement_init.adbcsqlite_connection <- function(connection, ...,
                                                      adbc.ingest.target_table = NULL,
                                                      adbc.ingest.target_catalog = NULL,
                                                      adbc.ingest.mode = NULL,
                                                      adbc.sqlite.query.batch_rows = NULL) {
  options <- list(
    ...,
    adbc.ingest.target_table = adbc.ingest.target_table,
    adbc.ingest.target_catalog = adbc.ingest.target_catalog,
    adbc.ingest.mode = adbc.ingest.mode,
    adbc.sqlite.query.batch_rows = adbc.sqlite.query.batch_rows
  )

  adbcdrivermanager::adbc_statement_init_default(
    connection,
    options,
    subclass = "adbcsqlite_statement"
  )
}
