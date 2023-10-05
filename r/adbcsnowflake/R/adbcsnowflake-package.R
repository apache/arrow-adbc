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
#' @aliases adbcsnowflake-package
"_PACKAGE"

## usethis namespace: start
#' @useDynLib adbcsnowflake, .registration = TRUE
## usethis namespace: end
NULL

#' ADBC Snowflake Driver
#'
#' @inheritParams adbcdrivermanager::adbc_database_init
#' @inheritParams adbcdrivermanager::adbc_connection_init
#' @inheritParams adbcdrivermanager::adbc_statement_init
#' @param uri A URI to a database path (e.g.,
#'   `user[:password]@account/database[?param1=value1]`)
#' @param adbc.connection.autocommit Use FALSE to disable the default
#'   autocommit behaviour.
#' @param adbc.ingest.target_table The name of the target table for a bulk insert.
#' @param adbc.ingest.mode Whether to create (the default) or append.
#' @param ... Extra key/value options passed to the driver.
#'
#' @return An [adbcdrivermanager::adbc_driver()]
#' @export
#'
#' @examples
#' adbcsnowflake()
#'
adbcsnowflake <- function() {
  adbcdrivermanager::adbc_driver(
    .Call(adbcsnowflake_c_snowflake),
    subclass = "adbcsnowflake_driver_snowflake"
  )
}

#' @rdname adbcsnowflake
#' @importFrom adbcdrivermanager adbc_database_init
#' @export
adbc_database_init.adbcsnowflake_driver_snowflake <- function(driver, ..., uri = NULL) {
  options <- list(..., uri = uri)
  adbcdrivermanager::adbc_database_init_default(
    driver,
    options,
    subclass = "adbcsnowflake_database"
  )
}

#' @rdname adbcsnowflake
#' @importFrom adbcdrivermanager adbc_connection_init
#' @export
adbc_connection_init.adbcsnowflake_database <- function(database, ...,
                                                        adbc.connection.autocommit = NULL) {
  options <- list(..., adbc.connection.autocommit = adbc.connection.autocommit)
  adbcdrivermanager::adbc_connection_init_default(
    database,
    options,
    subclass = "adbcsnowflake_connection"
  )
}

#' @rdname adbcsnowflake
#' @importFrom adbcdrivermanager adbc_statement_init
#' @export
adbc_statement_init.adbcsnowflake_connection <- function(connection, ...,
                                                         adbc.ingest.target_table = NULL,
                                                         adbc.ingest.mode = NULL) {
  options <- list(
    ...,
    adbc.ingest.target_table = adbc.ingest.target_table,
    adbc.ingest.mode = adbc.ingest.mode
  )

  adbcdrivermanager::adbc_statement_init_default(
    connection,
    options,
    subclass = "adbcsnowflake_statement"
  )
}
