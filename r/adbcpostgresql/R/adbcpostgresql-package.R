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
#' @aliases adbcpostgresql-package
"_PACKAGE"

## usethis namespace: start
#' @useDynLib adbcpostgresql, .registration = TRUE
## usethis namespace: end
NULL

#' ADBC PostgreSQL Driver
#'
#' @inheritParams adbcdrivermanager::adbc_database_init
#' @inheritParams adbcdrivermanager::adbc_connection_init
#' @inheritParams adbcdrivermanager::adbc_statement_init
#' @param uri A URI to a database path (e.g.,
#'   `postgresql://localhost:1234/postgres?user=user&password=password`)
#' @param adbc.connection.autocommit Use FALSE to disable the default
#'   autocommit behaviour.
#' @param adbc.ingest.target_table The name of the target table for a bulk insert.
#' @param adbc.ingest.target_db_schema The schema of the table for a bulk insert.
#' @param adbc.ingest.mode Whether to create (the default) or append.
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
adbc_database_init.adbcpostgresql_driver_postgresql <- function(driver, ..., uri) {
  adbcdrivermanager::adbc_database_init_default(
    driver,
    list(..., uri = uri),
    subclass = "adbcpostgresql_database"
  )
}

#' @rdname adbcpostgresql
#' @importFrom adbcdrivermanager adbc_connection_init
#' @export
adbc_connection_init.adbcpostgresql_database <- function(database, ...,
                                                         adbc.connection.autocommit = NULL) {
  options <- list(..., adbc.connection.autocommit = adbc.connection.autocommit)
  adbcdrivermanager::adbc_connection_init_default(
    database,
    options,
    subclass = "adbcpostgresql_connection"
  )
}

#' @rdname adbcpostgresql
#' @importFrom adbcdrivermanager adbc_statement_init
#' @export
adbc_statement_init.adbcpostgresql_connection <- function(connection, ...,
                                                      adbc.ingest.target_table = NULL,
                                                      adbc.ingest.target_db_schema = NULL,
                                                      adbc.ingest.mode = NULL) {
  options <- list(
    ...,
    adbc.ingest.target_table = adbc.ingest.target_table,
    adbc.ingest.target_db_schema = adbc.ingest.target_db_schema,
    adbc.ingest.mode = adbc.ingest.mode
  )

  adbcdrivermanager::adbc_statement_init_default(
    connection,
    options,
    subclass = "adbcpostgresql_statement"
  )
}
