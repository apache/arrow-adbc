
#' Databases
#'
#' @param driver An [adbc_driver()].
#' @param database An [adbc_database][adbc_database_init].
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
  database <- .Call(RAdbcDatabaseNew, driver$driver_init_func)
  if (!is.null(driver$name)) {
    adbc_database_set_options(
      database,
      c("driver" = driver$name, "entrypoint" = driver$entrypoint)
    )
  }

  database$driver <- driver
  adbc_database_set_options(database, options)

  error <- adbc_allocate_error()
  status <- .Call(RAdbcDatabaseInit, database, error)
  stop_for_error(status, error)
  class(database) <- c(subclass, class(database))
  database
}

#' @rdname adbc_database_init
#' @export
adbc_database_set_options <- function(database, options) {
  options <- key_value_options(options)
  error <- adbc_allocate_error()
  for (i in seq_along(options)) {
    key <- names(options)[i]
    value <- options[i]
    status <- .Call(
      RAdbcDatabaseSetOption,
      database,
      key,
      value,
      error
    )
    stop_for_error(status, error)
    xptr_add_option(database, key, value)
  }
  invisible(database)
}

#' @rdname adbc_database_init
#' @export
adbc_database_release <- function(database) {
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
  error <- adbc_allocate_error()
  status <- .Call(RAdbcConnectionInit, connection, database, error)
  stop_for_error(status, error)

  adbc_connection_set_options(connection, options)
  class(connection) <- c(subclass, class(connection))
  connection
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_set_options <- function(connection, options) {
  options <- key_value_options(options)
  error <- adbc_allocate_error()
  for (i in seq_along(options)) {
    key <- names(options)[i]
    value <- options[i]
    status <- .Call(
      RAdbcConnectionSetOption,
      connection,
      key,
      value,
      error
    )
    stop_for_error(status, error)
    xptr_add_option(connection, key, value)
  }
  invisible(connection)
}

#' @rdname adbc_connection_init
#' @export
adbc_connection_release <- function(connection) {
  error <- adbc_allocate_error()
  status <- .Call(RAdbcConnectionRelease, connection, error)
  stop_for_error(status, error)
  invisible(connection)
}


#' Connection methods
#'
#' @inheritParams adbc_connection_init
#' @param info_codes (Currently undocumented)
#' @param depth (Currently undocumented)
#' @param catalog (Currently undocumented)
#' @param db_schema (Currently undocumented)
#' @param table_name (Currently undocumented)
#' @param table_type (Currently undocumented)
#' @param column_name (Currently undocumented)
#' @param serialized_partition (Currently undocumented)
#'
#' @return
#'   - `adbc_connection_get_info()`, `adbc_connection_get_objects`,
#'     `adbc_connection_get_table_types()`, and `adbc_connection_read_partition()`
#'     return a [nanoarrow_array_stream][nanoarrow::as_nanoarrow_array_stream()].
#'   - `adbc_connection_get_table_schema()` returns a
#'     [nanoarrow_schena][nanoarrow::as_nanoarrow_schema()]
#'   - `adbc_connection_commit()` and `adbc_connection_rollback()` return
#'     `connection`, invisibly.
#' @export
#'
#' @examples
#' db <- adbc_database_init(adbc_driver_void())
#' con <- adbc_connection_init(db)
#' # (not implemented by the void driver)
#' try(adbc_connection_get_info(con, 0))
#'
adbc_connection_get_info <- function(connection, info_codes) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()
  status <- .Call(RAdbcConnectionGetInfo, connection, info_codes, out_stream, error)
  stop_for_error(status, error)

  out_stream
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_get_objects <- function(connection, depth, catalog, db_schema,
                                         table_name, table_type, column_name) {
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

  out_stream
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

  out_stream
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

  out_stream
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_commit <- function(connection) {
  error <- adbc_allocate_error()
  .Call(RAdbcConnectionCommit, connection, error)
  invisible(connection)
}

#' @rdname adbc_connection_get_info
#' @export
adbc_connection_rollback <- function(connection) {
  error <- adbc_allocate_error()
  .Call(RAdbcConnectionRollback, connection, error)
  invisible(connection)
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
  adbc_statement_set_options(statement, options)
  class(statement) <- c(subclass, class(statement))
  statement
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_set_options <- function(statement, options) {
  options <- key_value_options(options)
  error <- adbc_allocate_error()
  for (i in seq_along(options)) {
    key <- names(options)[i]
    value <- options[i]
    status <- .Call(
      RAdbcStatementSetOption,
      statement,
      key,
      value,
      error
    )
    stop_for_error(status, error)
    xptr_add_option(statement, key, value)
  }
  invisible(statement)
}

#' @rdname adbc_statement_init
#' @export
adbc_statement_release <- function(statement) {
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
adbc_statement_execute_query <- function(statement) {
  error <- adbc_allocate_error()
  out_stream <- nanoarrow::nanoarrow_allocate_array_stream()
  result <- .Call(RAdbcStatementExecuteQuery, statement, out_stream, error)
  stop_for_error(result$status, error)
  out_stream
}
