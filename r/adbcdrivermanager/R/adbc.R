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

#' Databases
#'
#' @param driver An [adbc_driver()].
#' @param database An [adbc_database][adbc_database_init].
#' @param option A specific option name
#' @param ... Driver-specific options. For the default method, these are
#'   named values that are converted to strings.
#' @param options A named `character()` or `list()` whose values are converted
#'   to strings.
#' @param subclass An extended class for an object so that drivers can specify
#'   finer-grained control over behaviour at the R level.
#'
#' @return An object of class adbc_database
#' @export
#'
#' @examples
#' adbc_database_init(adbc_driver_void())
#'
adbc_database_init <- function(driver, ...) {
  UseMethod("adbc_database_init")
}

#' @export
adbc_database_init.default <- function(driver, ...) {
  adbc_database_init_default(driver, list(...))
}

#' @rdname adbc_database_init
#' @export
adbc_database_init_default <- function(driver, options = NULL, subclass = character()) {
  database <- .Call(
    RAdbcDatabaseNew,
    driver$driver_init_func,
    driver$load_flags
  )

  if (!is.null(driver$name)) {
    adbc_database_set_options(
      database,
      c("driver" = driver$name, "entrypoint" = driver$entrypoint)
    )
  }

  database$driver <- driver

  with_adbc(database, {
    adbc_database_set_options(database, options)

    error <- adbc_allocate_error()
    status <- .Call(RAdbcDatabaseInit, database, error)
    stop_for_error(status, error)
    class(database) <- c(subclass, class(database))

    adbc_xptr_move(database)
  })
}

#' @rdname adbc_database_init
#' @export
adbc_database_release <- function(database) {
  stop_for_nonzero_child_count(database)

  error <- adbc_allocate_error()
  status <- .Call(RAdbcDatabaseRelease, database, error)
  stop_for_error(status, error)
  invisible(database)
}

#' Connections
#'
#' @inheritParams adbc_database_init
#' @param connection An [adbc_connection][adbc_connection_init]
#'
#' @return An object of class 'adbc_connection'
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_void())
#' adbc_connection_init(db)
#'
adbc_connection_init <- function(database, ...) {
  UseMethod("adbc_connection_init")
}

#' @export
adbc_connection_init.default <- function(database, ...) {
  adbc_connection_init_default(database, list(...))
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_init_default <- function(database, options = NULL, subclass = character()) {
  connection <- .Call(RAdbcConnectionNew)
  connection$database <- database

  with_adbc(connection, {
    adbc_connection_set_options(connection, options)

    error <- adbc_allocate_error()
    status <- .Call(RAdbcConnectionInit, connection, database, error)
    stop_for_error(status, error)

    class(connection) <- c(subclass, class(connection))

    adbc_xptr_move(connection)
  })
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_release <- function(connection) {
  stop_for_nonzero_child_count(connection)

  if (isTRUE(connection$.release_database)) {
    database <- connection$database
    on.exit(adbc_database_release(database))
  }

  error <- adbc_allocate_error()
  status <- .Call(RAdbcConnectionRelease, connection, error)
  stop_for_error(status, error)
  invisible(connection)
}


#' Connection methods
#'
#' @inheritParams adbc_connection_init
#' @param info_codes A list of metadata codes to fetch, or NULL to fetch all.
#'   Valid values are documented in the adbc.h header.
#' @param depth The level of nesting to display. If 0, display all levels. If 1,
#'   display only catalogs (i.e., catalog_schemas will be null). If 2, display
#'   only catalogs and schemas (i.e., db_schema_tables will be null). If 3,
#'   display only catalogs, schemas, and tables.
#' @param catalog Only show tables in the given catalog. If NULL, do not filter
#'   by catalog. If an empty string, only show tables without a catalog. May be
#'   a search pattern.
#' @param db_schema Only show tables in the given database schema. If NULL, do
#'   not filter by database schema. If an empty string, only show tables without
#'   a database schema. May be a search pattern.
#' @param table_name Constrain an object or statistics query for a specific table.
#'   If NULL, do not filter by name. May be a search pattern.
#' @param table_type Only show tables matching one of the given table types. If
#'   NULL, show tables of any type. Valid table types can be fetched from
#'   GetTableTypes. Terminate the list with a NULL entry.
#' @param column_name Only show columns with the given name. If NULL, do not
#'   filter by name. May be a search pattern.
#' @param serialized_partition The partition descriptor.
#' @param approximate If `FALSE`, request exact values of statistics,
#'   else allow for best-effort, approximate, or cached values. The database
#'   may return approximate values regardless, as indicated in the result.
#'   Requesting exact values may be expensive or unsupported.
#' @param value A string or identifier.
#'
#' @return
#'   - `adbc_connection_get_info()`, `adbc_connection_get_objects()`,
#' `adbc_connection_get_table_types()`, and `adbc_connection_read_partition()`
#' return a [nanoarrow_array_stream][nanoarrow::as_nanoarrow_array_stream()].
#'   - `adbc_connection_get_table_schema()` returns a
#' [nanoarrow_schena][nanoarrow::as_nanoarrow_schema()]
#'   - `adbc_connection_commit()` and `adbc_connection_rollback()` return
#' `connection`, invisibly.
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_void())
#' con <- adbc_connection_init(db)
#' # (not implemented by the void driver)
#' try(adbc_connection_get_info(con, 0))
#'
adbc_connection_get_info <- function(connection, info_codes = NULL) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()
  status <- .Call(
    RAdbcConnectionGetInfo,
    connection,
    info_codes,
    out_stream,
    error
  )
  stop_for_error(status, error)

  adbc_child_stream(connection, out_stream)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_get_objects <- function(connection, depth = 0L, catalog = NULL, db_schema = NULL,
                                        table_name = NULL, table_type = NULL, column_name = NULL) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()
  status <- .Call(
    RAdbcConnectionGetObjects,
    connection,
    depth,
    catalog,
    db_schema,
    table_name,
    table_type,
    column_name,
    out_stream,
    error
  )
  stop_for_error(status, error)

  adbc_child_stream(connection, out_stream)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_get_table_schema <- function(connection, catalog, db_schema, table_name) {
  error <- adbc_allocate_error()
  out_schema <- nanoarrow::nanoarrow_allocate_schema()
  status <- .Call(
    RAdbcConnectionGetTableSchema,
    connection,
    catalog,
    db_schema,
    table_name,
    out_schema,
    error
  )
  stop_for_error(status, error)

  out_schema
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_get_table_types <- function(connection) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()
  status <- .Call(RAdbcConnectionGetTableTypes, connection, out_stream, error)
  stop_for_error(status, error)

  adbc_child_stream(connection, out_stream)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_read_partition <- function(connection, serialized_partition) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()
  status <- .Call(
    RAdbcConnectionReadPartition,
    connection,
    serialized_partition,
    out_stream,
    error
  )
  stop_for_error(status, error)

  adbc_child_stream(connection, out_stream)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_commit <- function(connection) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcConnectionCommit, connection, error)
  stop_for_error(status, error)
  invisible(connection)
}


#' @rdname adbc_connection_get_info
#' @export
adbc_connection_rollback <- function(connection) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcConnectionRollback, connection, error)
  stop_for_error(status, error)
  invisible(connection)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_cancel <- function(connection) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcConnectionCancel, connection, error)
  stop_for_error(status, error)
  invisible(connection)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_get_statistic_names <- function(connection) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()
  status <- .Call(RAdbcConnectionGetStatisticNames, connection, out_stream, error)
  stop_for_error(status, error)

  adbc_child_stream(connection, out_stream)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_get_statistics <- function(connection, catalog, db_schema,
                                           table_name, approximate = FALSE) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()

  status <- .Call(
    RAdbcConnectionGetStatistics,
    connection,
    catalog,
    db_schema,
    table_name,
    approximate,
    out_stream,
    error
  )
  stop_for_error(status, error)

  adbc_child_stream(connection, out_stream)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_quote_identifier <- function(connection, value, ...) {
  UseMethod("adbc_connection_quote_identifier")
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_quote_string <- function(connection, value, ...) {
  UseMethod("adbc_connection_quote_string")
}

#' @export
adbc_connection_quote_identifier.default <- function(connection, value, ...) {
  out <- gsub('"', '""', enc2utf8(value))
  paste0('"', out, '"')
}

#' @export
adbc_connection_quote_string.default <- function(connection, value, ...) {
  out <- gsub("'", "''", enc2utf8(value))
  paste0("'", out, "'")
}

#' Statements
#'
#' @inheritParams adbc_connection_init
#' @param statement An [adbc_statement][adbc_statement_init]
#'
#' @return An object of class 'adbc_statement'
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_void())
#' con <- adbc_connection_init(db)
#' adbc_statement_init(con)
#'
adbc_statement_init <- function(connection, ...) {
  UseMethod("adbc_statement_init")
}

#' @export
adbc_statement_init.default <- function(connection, ...) {
  adbc_statement_init_default(connection, list(...))
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_init_default <- function(connection, options = NULL, subclass = character()) {
  statement <- .Call(RAdbcStatementNew, connection)
  statement$connection <- connection

  with_adbc(statement, {
    adbc_statement_set_options(statement, options)
    class(statement) <- c(subclass, class(statement))
    adbc_xptr_move(statement)
  })
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_release <- function(statement) {
  stop_for_nonzero_child_count(statement)

  if (isTRUE(statement$.release_connection)) {
    connection <- statement$connection
    on.exit(adbc_connection_release(connection))
  }

  error <- adbc_allocate_error()
  status <- .Call(RAdbcStatementRelease, statement, error)
  stop_for_error(status, error)
  invisible(statement)
}


#' Statement methods
#'
#' @inheritParams adbc_statement_init
#' @param query An SQL query as a string
#' @param plan A raw vector representation of a serialized Substrait plan.
#' @param values A [nanoarrow_array][nanoarrow::as_nanoarrow_array] or object
#'   that can be coerced to one.
#' @param stream A [nanoarrow_array_stream][nanoarrow::as_nanoarrow_array_stream]
#'   or object that can be coerced to one.
#' @param schema A [nanoarrow_schema][nanoarrow::as_nanoarrow_schema] or object
#'   that can be coerced to one.
#' @param stream_join_parent Use `TRUE` to invalidate `statement` and tie its
#'   lifecycle to `stream`.
#'
#' @return
#'   - `adbc_statement_set_sql_query()`, `adbc_statement_set_substrait_plan()`,
#'     `adbc_statement_prepare()`, `adbc_statement_bind()`,
#'     `adbc_statement_bind_stream()`, and `adbc_statement_execute_query()`
#'     return `statement`, invisibly.
#'   - `adbc_statement_get_parameter_schema()` returns a
#'     [nanoarrow_schema][nanoarrow::as_nanoarrow_schema].
#'
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_void())
#' con <- adbc_connection_init(db)
#' stmt <- adbc_statement_init(con)
#' # (not implemented by the void driver)
#' try(adbc_statement_set_sql_query(stmt, "some query"))
#'
adbc_statement_set_sql_query <- function(statement, query) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcStatementSetSqlQuery, statement, query, error)
  stop_for_error(status, error)
  invisible(statement)
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_set_substrait_plan <- function(statement, plan) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcStatementSetSubstraitPlan, statement, plan, error)
  stop_for_error(status, error)
  invisible(statement)
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_prepare <- function(statement) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcStatementPrepare, statement, error)
  stop_for_error(status, error)
  invisible(statement)
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_get_parameter_schema <- function(statement) {
  error <- adbc_allocate_error()
  schema <- nanoarrow::nanoarrow_allocate_schema()
  status <- .Call(RAdbcStatementGetParameterSchema, statement, schema, error)
  stop_for_error(status, error)
  schema
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_bind <- function(statement, values, schema = NULL) {
  values <- nanoarrow::as_nanoarrow_array(values, schema = schema)
  schema <- nanoarrow::infer_nanoarrow_schema(values)
  error <- adbc_allocate_error()
  status <- .Call(RAdbcStatementBind, statement, values, schema, error)
  stop_for_error(status, error)
  invisible(statement)
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_bind_stream <- function(statement, stream, schema = NULL) {
  stream <- nanoarrow::as_nanoarrow_array_stream(stream, schema = schema)
  error <- adbc_allocate_error()
  status <- .Call(RAdbcStatementBindStream, statement, stream, error)
  stop_for_error(status, error)
  invisible(statement)
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_execute_query <- function(statement, stream = NULL,
                                         stream_join_parent = FALSE) {
  error <- adbc_allocate_error()

  if (is.null(stream)) {
    result <- .Call(RAdbcStatementExecuteQuery, statement, NULL, error)
  } else {
    stream_tmp <- nanoarrow::nanoarrow_allocate_array_stream()
    result <- .Call(RAdbcStatementExecuteQuery, statement, stream_tmp, error)
    if (identical(result$status, 0L)) {
      stream_tmp <- adbc_child_stream(
        statement,
        stream_tmp,
        release_parent = stream_join_parent
      )
      nanoarrow::nanoarrow_pointer_export(stream_tmp, stream)
    }
  }

  stop_for_error(result$status, error)
  result$rows_affected
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_execute_schema <- function(statement) {
  error <- adbc_allocate_error()
  out_schema <- nanoarrow::nanoarrow_allocate_schema()

  status <- .Call(RAdbcStatementExecuteSchema, statement, out_schema, error)
  stop_for_error(status, error)

  out_schema
}

#' @rdname adbc_statement_set_sql_query
#' @export
adbc_statement_cancel <- function(statement) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcStatementCancel, statement, error)
  stop_for_error(status, error)
  invisible(statement)
}
