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
#' Useful for debugging another driver or ensuring that certain calls take
#' occur/return specific values.
#'
#' @param parent_driver An [adbc_driver()] exposed as a `driver_init_func`.
#'   Drivers in the form of DLL/entrypoint are not currently supported.
#' @param tag A message prefix.
#'
#' @return An object of class 'adbc_driver_log'
#' @export
#'
#' @examples
#' drv <- adbc_driver_log(adbc_driver_void())
#' db <- adbc_database_init(drv)
#' con <- adbc_connection_init(db)
#' stmt <- adbc_statement_init(con, mtcars)
#' try(adbc_statement_execute_query(stmt))
#' adbc_statement_release(stmt)
#' adbc_connection_release(con)
#' adbc_database_release(db)
#'
adbc_driver_log <- function(parent_driver,
                            tag = "LogDriver") {
  adbc_driver(
    .Call(RAdbcLogDriverInitFunc),
    parent_driver = parent_driver,
    tag = tag,
    subclass = "adbc_driver_log"
  )
}

#' @export
adbc_database_init.adbc_driver_log <- function(driver, ...) {
  init_func_addr_chr <- nanoarrow::nanoarrow_pointer_addr_chr(
    driver$parent_driver$driver_init_func
  )

  adbc_database_init_default(
    driver,
    options = list(
      adbc.r.logdriver.tag = driver$tag,
      adbc.r.logdriver.driver_init_func_addr = init_func_addr_chr
    ),
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
adbc_statement_init.adbc_connection_log <- function(connection, stream = NULL, ...) {
  adbc_statement_init_default(
    connection,
    options = list(...),
    subclass = "adbc_statement_log"
  )
}
