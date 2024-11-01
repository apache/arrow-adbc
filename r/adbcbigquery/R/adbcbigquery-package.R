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
#' @aliases adbcbigquery-package
"_PACKAGE"

## usethis namespace: start
#' @useDynLib adbcbigquery, .registration = TRUE
## usethis namespace: end
NULL

#' ADBC BigQuery Driver
#'
#' @inheritParams adbcdrivermanager::adbc_database_init
#' @inheritParams adbcdrivermanager::adbc_connection_init
#' @inheritParams adbcdrivermanager::adbc_statement_init
#' @param token A token obtained from [bigrquery::bq_token()] or
#'   [gargle::token_fetch()]. This is the easiest way to authenticate.
#' @param ... Extra key/value options passed to the driver.
#'
#' @return An [adbcdrivermanager::adbc_driver()]
#' @export
#'
#' @examples
#' adbcbigquery()
#'
adbcbigquery <- function() {
  adbcdrivermanager::adbc_driver(
    .Call(adbcbigquery_c_bigquery),
    subclass = "adbcbigquery_driver_bigquery"
  )
}

#' @rdname adbcbigquery
#' @importFrom adbcdrivermanager adbc_database_init
#' @export
adbc_database_init.adbcbigquery_driver_bigquery <- function(driver, ..., token = NULL) {
  if (!is.null(token)) {
    options <- list(options_from_token(token), ...)
  } else {
    options <- list(...)
  }

  adbcdrivermanager::adbc_database_init_default(
    driver,
    options,
    subclass = "adbcbigquery_database"
  )
}

#' @rdname adbcbigquery
#' @importFrom adbcdrivermanager adbc_connection_init
#' @export
adbc_connection_init.adbcbigquery_database <- function(database, ...) {
  options <- list(...)

  adbcdrivermanager::adbc_connection_init_default(
    database,
    options,
    subclass = "adbcbigquery_connection"
  )
}

#' @rdname adbcbigquery
#' @importFrom adbcdrivermanager adbc_statement_init
#' @export
adbc_statement_init.adbcbigquery_connection <- function(connection, ...) {
  options <- list(...)

  adbcdrivermanager::adbc_statement_init_default(
    connection,
    options,
    subclass = "adbcbigquery_statement"
  )
}

# Converts a bigrquery::bq_token() or garge::token_fetch() into the options
# required by the ADBC driver.
options_from_token <- function(tok) {
  if (inherits(tok, "request")) {
    tok <- tok$auth_token
  }

  if (!inherits(tok, "Token2.0")) {
    stop(
      sprintf(
        "ADBC BigQuery database options from token failed (expected Token2.0, got %s)",
        paste(class(tok), sep = " / ")
      )
    )
  }

  # Because adbcdrivermanager:::key_value_options() is not exposed, we create the
  # options ourselves such that set_options() can automatically expand them.
  structure(
    c(
      "adbc.bigquery.sql.auth_type" = "adbc.bigquery.sql.auth_type.user_authentication",
      "adbc.bigquery.sql.auth.client_id" = tok$client$id,
      "adbc.bigquery.sql.auth.client_secret" = tok$client$secret,
      "adbc.bigquery.sql.auth.refresh_token" = tok$credentials$refresh_token
      # Ideally we would also pass on tok$credentials$access_token; however,
      # the underlying Go driver does not yet implement the ability to pass this
      # information directly (i.e., it will always refresh on connect)
    ),
    class = "adbc_options"
  )
}
