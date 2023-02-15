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
"_PACKAGE"

## usethis namespace: start
#' @useDynLib adbcsqlite, .registration = TRUE
## usethis namespace: end
NULL

#' ADBC SQLite3 Driver
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

#' @export
adbc_database_init.adbcsqlite_driver_sqlite <- function(driver, uri = ":memory:") {
  adbcdrivermanager::adbc_database_init_default(
    driver,
    list(uri = uri),
    subclass = "adbcsqlite_database"
  )
}

#' @export
adbc_connection_init.adbcsqlite_database <- function(database,
                                                     adbc.connection.autocommit = NULL) {
  options <- list(adbc.connection.autocommit = adbc.connection.autocommit)
  adbcdrivermanager::adbc_connection_init_default(
    database,
    options[!vapply(options, is.null, logical(1))],
    subclass = "adbcsqlite_connection"
  )
}

#' @export
adbc_statement_init.adbcsqlite_connection <- function(connection, stream,
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
    subclass = "adbcsqlite_statement"
  )
}
