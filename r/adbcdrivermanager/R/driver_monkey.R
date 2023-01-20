
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
#' result <- adbc_statement_execute_query(stmt)
#' as.data.frame(result$get_next())
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
adbc_statement_init.adbc_connection_monkey <- function(connection, stream, ...) {
  stream <- nanoarrow::as_nanoarrow_array_stream(stream)
  options <- list(
    result_stream_address = nanoarrow::nanoarrow_pointer_addr_chr(stream),
    ...
  )
  adbc_statement_init_default(connection, options, subclass = "adbc_statement_monkey")
}
