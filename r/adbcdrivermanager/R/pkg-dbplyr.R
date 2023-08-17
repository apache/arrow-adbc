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
#' @examplesIf requireNamespace("dbplyr", quietly = TRUE)
#' db <- adbc_database_init(adbc_driver_void())
#' con <- adbc_connection_init(db)
#' adbc_simulate_dbi(con)
#'
adbc_simulate_dbi <- function(connection, method = NULL, ...) {
  UseMethod("adbc_simulate_dbi")
}

#' @export
adbc_simulate_dbi.default <- function(connection, method = NULL, ...) {
  dbplyr::simulate_dbi()
}

# registered in zzz.R
sql_translation.adbc_connection <- function(con, ...) {
  dbplyr::sql_translation(
    adbc_simulate_dbi(con, method = "dbplyr::sql_translation"),
    ...
  )
}

db_connection_describe.adbc_connection <- function(con, ...) {
  dbplyr::db_connection_describe(
    adbc_simulate_dbi(con, method = "dbplyr::db_connection_describe"),
    ...
  )
}

dbplyr_edition.adbc_connection <- function(con) 2L

# nocov start
sql_table_analyze.adbc_connection <- function(con, ...) {
  dbplyr::sql_table_analyze(
    adbc_simulate_dbi(con, method = "dbplyr::sql_table_analyze"),
    ...
  )
}

sql_table_index.adbc_connection <- function(con, ...) {
  dbplyr::sql_table_index(
    adbc_simulate_dbi(con, method = "dbplyr::sql_table_index"),
    ...
  )
}

sql_query_explain.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_explain(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_explain"),
    ...
  )
}

sql_query_fields.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_fields(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_fields"),
    ...
  )
}

sql_query_save.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_save(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_save"),
    ...
  )
}

sql_query_select.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_select(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_select"),
    ...
  )
}

sql_query_join.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_join(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_join"),
    ...
  )
}

sql_query_semi_join.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_semi_join(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_semi_join"),
    ...
  )
}

sql_query_set_op.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_set_op(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_set_op"),
    ...
  )
}

sql_query_wrap.adbc_connection <- function(con, ...) {
  dbplyr::sql_query_query_wrap(
    adbc_simulate_dbi(con, method = "dbplyr::sql_query_wrap"),
    ...
  )
}

db_table_temporary.adbc_connection <- function(con, ...) {
  dbplyr::db_table_temporary(
    adbc_simulate_dbi(con, method = "dbplyr::db_table_temporary"),
    ...
  )
}

sql_expr_matches.adbc_connection <- function(con, ...) {
  dbplyr::sql_expr_matches(
    adbc_simulate_dbi(con, method = "dbplyr::sql_expr_matches"),
    ...
  )
}

sql_join_suffix.adbc_connection <- function(con, ...) {
  dbplyr::sql_join_suffix(
    adbc_simulate_dbi(con, method = "dbplyr::sql_join_suffix"),
    ...
  )
}
# nocov end
