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

#' Log calls to another driver
#'
#' Useful for debugging or ensuring that certain calls occur during
#' initialization and/or cleanup. The current logging output should not
#' be considered stable and may change in future releases.
#'
#'
#' @return An object of class 'adbc_driver_log'
#' @export
#'
#' @examples
#' drv <- adbc_driver_log()
#' db <- adbc_database_init(drv, key = "value")
#' con <- adbc_connection_init(db, key = "value")
#' stmt <- adbc_statement_init(con, key = "value")
#' try(adbc_statement_execute_query(stmt))
#' adbc_statement_release(stmt)
#' adbc_connection_release(con)
#' adbc_database_release(db)
#'
adbc_driver_log <- function() {
  if (is.null(internal_driver_env$log)) {
    internal_driver_env$log <- adbc_driver(
      .Call(RAdbcLogDriverInitFunc),
      subclass = "adbc_driver_log"
    )
  }

  internal_driver_env$log
}

#' @export
adbc_database_init.adbc_driver_log <- function(driver, ...) {
  adbc_database_init_default(
    driver,
    options = list(...),
    subclass = "adbc_database_log"
  )
}

#' @export
adbc_connection_init.adbc_database_log <- function(database, ...) {
  adbc_connection_init_default(
    database,
    options = list(...),
    subclass = "adbc_connection_log"
  )
}

#' @export
adbc_statement_init.adbc_connection_log <- function(connection, ...) {
  adbc_statement_init_default(
    connection,
    options = list(...),
    subclass = "adbc_statement_log"
  )
}
