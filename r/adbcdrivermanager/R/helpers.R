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

#' Read, write, and execute on ADBC connections
#'
#' These are convenience methods useful for testing connections. Note that
#' S3 dispatch is always on `db_or_con` (i.e., drivers may provide their own
#' implementations).
#'
#' @param db_or_con An adbc_database or adbc_connection. If a database, a
#'   connection will be opened. For `read_adbc()`, this connection will
#'   be closed when the resulting stream has been released.
#' @param tbl A data.frame, [nanoarrow_array][nanoarrow::as_nanoarrow_array],
#'   or  [nanoarrow_array_stream][nanoarrow::as_nanoarrow_array_stream].
#' @param target_table A target table name to which `tbl` should be written.
#' @param mode One of "create", "append", or "default" (error if the schema
#'   is not compatible or append otherwise).
#' @param query An SQL query
#' @param bind A data.frame, nanoarrow_array, or nanoarrow_array_stream of
#'   bind parameters or NULL to skip the bind/prepare step.
#' @param temporary Use TRUE to create a table as a temporary table.
#' @param ... Passed to S3 methods.
#'
#' @return
#'   - `read_adbc()`: A [nanoarrow_array_stream][nanoarrow::as_nanoarrow_array_stream]
#'   - `execute_adbc()`: `db_or_con`, invisibly.
#'   - `write_adbc()`: `tbl`, invisibly.
#' @export
#'
#' @examples
#' # On a database, connections are opened and closed
#' db <- adbc_database_init(adbc_driver_log())
#' try(read_adbc(db, "some sql"))
#' try(execute_adbc(db, "some sql"))
#' try(write_adbc(mtcars, db, "some_table"))
#'
#' # Also works on a connection
#' con <- adbc_connection_init(db)
#' try(read_adbc(con, "some sql"))
#' try(execute_adbc(con, "some sql"))
#' try(write_adbc(mtcars, con, "some_table"))
#'
read_adbc <- function(db_or_con, query, ..., bind = NULL) {
  UseMethod("read_adbc")
}

#' @rdname read_adbc
#' @export
execute_adbc <- function(db_or_con, query, ..., bind = NULL) {
  UseMethod("execute_adbc")
}

#' @rdname read_adbc
#' @export
write_adbc <- function(tbl, db_or_con, target_table, ...,
                       mode = c("default", "create", "append"),
                       temporary = FALSE) {
  UseMethod("write_adbc", db_or_con)
}


#' @export
read_adbc.default <- function(db_or_con, query, ..., bind = NULL) {
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  execute_adbc(db_or_con, query, ..., bind = bind, stream = stream)
  stream
}

#' @export
execute_adbc.default <- function(db_or_con, query, ..., bind = NULL, stream = NULL) {
  assert_adbc(db_or_con, c("adbc_database", "adbc_connection"))

  if (inherits(db_or_con, "adbc_database")) {
    con <- adbc_connection_init(db_or_con)
    on.exit(adbc_connection_release(con))

    stmt <- adbc_statement_init(con)
    adbc_statement_join(stmt, con)

  } else {
    con <- db_or_con
    stmt <- adbc_statement_init(con)
  }

  on.exit(adbc_release_non_null(stmt))
  adbc_statement_set_sql_query(stmt, query)

  if (!is.null(bind)) {
    adbc_statement_bind_stream(stmt, bind)
    adbc_statement_prepare(stmt)
  }

  adbc_statement_execute_query(stmt, stream, stream_join_parent = TRUE)

  invisible(db_or_con)
}

#' @export
write_adbc.default <- function(tbl, db_or_con, target_table, ...,
                               mode = c("default", "create", "append"),
                               temporary = FALSE) {
  assert_adbc(db_or_con, c("adbc_database", "adbc_connection"))
  mode <- match.arg(mode)

  if (inherits(db_or_con, "adbc_database")) {
    con <- adbc_connection_init(db_or_con)
    on.exit(adbc_connection_release(con))
  } else {
    con <- db_or_con
  }

  stmt <- adbc_statement_init(
    con,
    adbc.ingest.target_table = target_table,
    adbc.ingest.mode = if (!identical(mode, "default")) paste0("adbc.ingest.mode.", mode),
    adbc.ingest.temporary = if (temporary) "true"
  )
  on.exit(adbc_statement_release(stmt), add = TRUE, after = FALSE)

  adbc_statement_bind_stream(stmt, tbl)
  adbc_statement_execute_query(stmt)
  invisible(tbl)
}


#' Cleanup helpers
#'
#' Managing the lifecycle of databases, connections, and statements can
#' be complex and error-prone. The R objects that wrap the underlying ADBC
#' pointers will perform cleanup in the correct order if you rely on garbage
#' collection (i.e., do nothing and let the objects go out of scope); however
#' it is good practice to explicitly clean up these objects. These helpers
#' are designed to make explicit and predictable cleanup easy to accomplish.
#'
#' Note that you can use [adbc_connection_join()] and [adbc_statement_join()]
#' to tie the lifecycle of the parent object to that of the child object.
#' These functions mark any previous references to the parent object as
#' released so you can still use local and with helpers to manage the parent
#' object before it is joined. Use `stream_join_parent = TRUE` in
#' [adbc_statement_execute_query()] to tie the lifecycle of a statement to
#' the output stream.
#'
#' @param x An ADBC database, ADBC connection, ADBC statement, or
#'   nanoarrow_array_stream returned from calls to an ADBC function.
#' @param code Code to execute before cleaning up the input.
#' @param .local_envir The execution environment whose scope should be tied
#'   to the input.
#'
#' @return
#'   - `with_adbc()` returns the result of `code`
#'   - `local_adbc()` returns the input, invisibly.
#' @export
#'
#' @examples
#' # Using with_adbc():
#' with_adbc(db <- adbc_database_init(adbc_driver_void()), {
#'   with_adbc(con <- adbc_connection_init(db), {
#'     with_adbc(stmt <- adbc_statement_init(con), {
#'       # adbc_statement_set_sql_query(stmt, "SELECT * FROM foofy")
#'       # adbc_statement_execute_query(stmt)
#'       "some result"
#'     })
#'   })
#' })
#'
#' # Using local_adbc_*() (works best within a function, test, or local())
#' local({
#'   db <- local_adbc(adbc_database_init(adbc_driver_void()))
#'   con <- local_adbc(adbc_connection_init(db))
#'   stmt <- local_adbc(adbc_statement_init(con))
#'   # adbc_statement_set_sql_query(stmt, "SELECT * FROM foofy")
#'   # adbc_statement_execute_query(stmt)
#'   "some result"
#' })
#'
with_adbc <- function(x, code) {
  assert_adbc(x)

  on.exit(adbc_release_non_null(x))
  force(code)
}

#' @rdname with_adbc
#' @export
local_adbc <- function(x, .local_envir = parent.frame()) {
  assert_adbc(x)

  withr::defer(adbc_release_non_null(x), envir = .local_envir)
  invisible(x)
}

#' Join the lifecycle of a unique parent to its child
#'
#' It is occasionally useful to return a connection, statement, or stream
#' from a function that was created from a unique parent. These helpers
#' tie the lifecycle of a unique parent object to its child such that the
#' parent object is released predictably and immediately after the child.
#' These functions will invalidate all references to the previous R object.
#'
#' @param database A database created with [adbc_database_init()]
#' @param connection A connection created with [adbc_connection_init()]
#' @param statement A statement created with [adbc_statement_init()]
#'
#' @return The input, invisibly.
#' @export
#'
#' @examples
#' # Use local_adbc to ensure prompt cleanup on error;
#' # use join functions to return a single object that manages
#' # the lifecycle of all three.
#' stmt <- local({
#'   db <- local_adbc(adbc_database_init(adbc_driver_log()))
#'
#'   con <- local_adbc(adbc_connection_init(db))
#'   adbc_connection_join(con, db)
#'
#'   stmt <- local_adbc(adbc_statement_init(con))
#'   adbc_statement_join(stmt, con)
#'
#'   adbc_xptr_move(stmt)
#' })
#'
#' # Everything is released immediately when the last object is released
#' adbc_statement_release(stmt)
#'
adbc_connection_join <- function(connection, database) {
  assert_adbc(connection, "adbc_connection")

  stopifnot(
    identical(database, connection$database),
    identical(database$.child_count, 1L)
  )

  connection$.release_database <- TRUE
  connection$database <- adbc_xptr_move(database, check_child_count = FALSE)
  xptr_set_protected(connection, connection$database)
  invisible(connection)
}

#' @rdname adbc_connection_join
#' @export
adbc_statement_join <- function(statement, connection) {
  assert_adbc(statement, "adbc_statement")

  stopifnot(
    identical(connection, statement$connection),
    identical(connection$.child_count, 1L)
  )

  statement$.release_connection <- TRUE
  statement$connection <- adbc_xptr_move(connection, check_child_count = FALSE)
  xptr_set_protected(statement, statement$connection)
  invisible(statement)
}

adbc_child_stream <- function(parent, stream, release_parent = FALSE) {
  assert_adbc(parent)

  # This finalizer will run immediately on release (if released explicitly
  # on the main R thread) or on garbage collection otherwise.
  self_contained_finalizer <- function() {
    try({
      parent$.child_count <- parent$.child_count - 1L
      if (release_parent) {
        adbc_release_non_null(parent)
      }
    })
  }

  # Make sure we don't keep any variables around that aren't needed
  # for the finalizer and make sure we do keep around a strong reference
  # to parent.
  self_contained_finalizer_env <- as.environment(
    list(
      parent = if (release_parent) adbc_xptr_move(parent) else parent,
      release_parent = release_parent
    )
  )
  parent.env(self_contained_finalizer_env) <- asNamespace("adbcdrivermanager")
  environment(self_contained_finalizer) <- self_contained_finalizer_env

  # Set the finalizer using nanoarrow's method for this
  stream_out <- nanoarrow::array_stream_set_finalizer(
    stream,
    self_contained_finalizer
  )

  # Once we're sure this will succeed, increment the parent child count
  # Use whatever version is in the finalizer env (we might have moved parent)
  self_contained_finalizer_env$parent$.child_count <-
    self_contained_finalizer_env$parent$.child_count + 1L
  stream_out
}
