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

#' Monkey see, monkey do!
#'
#' A driver whose query results are set in advance.
#'
#' @return An object of class 'adbc_driver_monkey'
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_monkey())
#' con <- adbc_connection_init(db)
#' stmt <- adbc_statement_init(con, mtcars)
#' stream <- nanoarrow::nanoarrow_allocate_array_stream()
#' adbc_statement_execute_query(stmt, stream)
#' as.data.frame(stream$get_next())
#'
adbc_driver_monkey <- function() {
  if (is.null(internal_driver_env$monkey)) {
    internal_driver_env$monkey <- adbc_driver(
      .Call(RAdbcMonkeyDriverInitFunc),
      subclass = "adbc_driver_monkey"
    )
  }

  internal_driver_env$monkey
}

#' @export
adbc_database_init.adbc_driver_monkey <- function(driver, ...) {
  adbc_database_init_default(driver, subclass = "adbc_database_monkey")
}

#' @export
adbc_connection_init.adbc_database_monkey <- function(database, ...) {
  adbc_connection_init_default(database, subclass = "adbc_connection_monkey")
}

#' @export
adbc_statement_init.adbc_connection_monkey <- function(connection, stream = NULL, ...) {
  stmt <- adbc_statement_init_default(connection, subclass = "adbc_statement_monkey")

  if (!is.null(stream)) {
    adbc_statement_bind_stream(stmt, stream)
  }

  stmt
}
