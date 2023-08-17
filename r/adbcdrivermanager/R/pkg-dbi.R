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

#' Simulate an equivalent DBI connection
#'
#' @inheritParams adbc_connection_init
#' @param method The package-qualified name of the method that
#'   requires a simulation of a DBI object.
#' @param ... Passed to S3 methods
#'
#' @return An S3 object like that returned by [dbplyr::simulate_dbi()]
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_void())
#' con <- adbc_connection_init(db)
#' adbc_simulate_dbi(con)
#'
adbc_simulate_dbi <- function(connection, method = NULL, ...) {
  UseMethod("adbc_simulate_dbi")
}

#' @export
adbc_simulate_dbi.default <- function(connection, method = NULL, ...) {
  adbc_simulate_dbi_default()
}

#' @rdname adbc_simulate_dbi
#' @export
adbc_simulate_dbi_default <- function(class = character()) {
  structure(list(), class = c(class, "TestConnection", "DBIConnection"))
}
