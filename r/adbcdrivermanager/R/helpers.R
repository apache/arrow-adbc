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

#' Cleanup helpers
#'
#' Managing the lifecycle of databases, connections, and statements can
#' be complex and error-prone. The R objects that wrap the underlying ADBC
#' pointers will perform cleanup in the correct order if you rely on garbage
#' collection (i.e., do nothing and let the objects go out of scope); however
#' it is good practice to explicitly clean up these objects. These helpers
#' are designed to make explicit and predictable cleanup easy to accomplish.
#'
#' Note that you can use [adbc_connection_join_database()],
#' [adbc_statement_join_connection()], and [adbc_stream_join_statement()]
#' to tie the lifecycle of the parent object to that of the child object.
#' These functions mark any previous references to the parent object as
#' released so you can still use local and with helpers to manage the parent
#' object before it is joined.
#'
#' @param database A database created with [adbc_database_init()]
#' @param connection A connection created with [adbc_connection_init()]
#' @param statement A statement created with [adbc_statement_init()]
#' @param code Code to execute before cleaning up the input.
#' @param .local_envir The execution environment whose scope should be tied
#'   to the input.
#'
#' @return
#'   - `with_*()` variants return the result of `code`
#'   - `local_*()` variants return the input, invisibly.
#' @export
#'
#' @examples
#' # Using with_adbc_*():
#' with_adbc_database(db <- adbc_database_init(adbc_driver_void()), {
#'   with_adbc_connection(con <- adbc_connection_init(db), {
#'     with_adbc_statement(stmt <- adbc_statement_init(con), {
#'       # adbc_statement_set_sql_query(stmt, "SELECT * FROM foofy")
#'       # adbc_statement_execute_query(stmt)
#'     })
#'   })
#' })
#'
#' # Using local_adbc_*() (works best within a function, test, or local())
#' local({
#'   db <- local_adbc_database(adbc_database_init(adbc_driver_void()))
#'   con <- local_adbc_connection(adbc_connection_init(db))
#'   stmt <- local_adbc_statement(adbc_statement_init(con))
#'   # adbc_statement_set_sql_query(stmt, "SELECT * FROM foofy")
#'   # adbc_statement_execute_query(stmt)
#' })
#'
with_adbc_database <- function(database, code) {
  if (!inherits(database, "adbc_database")) {
    stop("`database` must inherit from 'adbc_database'")
  }

  on.exit(adbc_database_release(database))
  force(code)
}

#' @rdname with_adbc_database
#' @export
with_adbc_connection <- function(connection, code) {
  if (!inherits(connection, "adbc_connection")) {
    stop("`connection` must inherit from 'adbc_connection'")
  }

  on.exit(adbc_connection_release(connection))
  force(code)
}

#' @rdname with_adbc_database
#' @export
with_adbc_statement <- function(statement, code) {
  if (!inherits(statement, "adbc_statement")) {
    stop("`statement` must inherit from 'adbc_statement'")
  }

  on.exit(adbc_statement_release(statement))
  force(code)
}

#' @rdname with_adbc_database
#' @export
local_adbc_database <- function(database, .local_envir = parent.frame()) {
  if (!inherits(database, "adbc_database")) {
    stop("`database` must inherit from 'adbc_database'")
  }

  withr::defer(adbc_database_release(database), envir = .local_envir)
  invisible(database)
}

#' @rdname with_adbc_database
#' @export
local_adbc_connection <- function(connection, .local_envir = parent.frame()) {
  if (!inherits(connection, "adbc_connection")) {
    stop("`connection` must inherit from 'adbc_connection'")
  }

  withr::defer(adbc_connection_release(connection), envir = .local_envir)
  invisible(connection)
}

#' @rdname with_adbc_database
#' @export
local_adbc_statement <- function(statement, .local_envir = parent.frame()) {
  if (!inherits(statement, "adbc_statement")) {
    stop("`statement` must inherit from 'adbc_statement'")
  }

  withr::defer(adbc_statement_release(statement), envir = .local_envir)
  invisible(statement)
}
