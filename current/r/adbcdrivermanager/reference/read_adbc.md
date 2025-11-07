# Read, write, and execute on ADBC connections

These are convenience methods useful for testing connections. Note that
S3 dispatch is always on `db_or_con` (i.e., drivers may provide their
own implementations).

## Usage

``` r
read_adbc(db_or_con, query, ..., bind = NULL)

execute_adbc(db_or_con, query, ..., bind = NULL)

write_adbc(
  tbl,
  db_or_con,
  target_table,
  ...,
  mode = c("default", "create", "append", "replace", "create_append"),
  temporary = FALSE
)
```

## Arguments

- db_or_con:

  An adbc_database or adbc_connection. If a database, a connection will
  be opened. For `read_adbc()`, this connection will be closed when the
  resulting stream has been released.

- query:

  An SQL query

- ...:

  Passed to S3 methods.

- bind:

  A data.frame, nanoarrow_array, or nanoarrow_array_stream of bind
  parameters or NULL to skip the bind/prepare step.

- tbl:

  A data.frame,
  [nanoarrow_array](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array.html),
  or
  [nanoarrow_array_stream](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array_stream.html).

- target_table:

  A target table name to which `tbl` should be written.

- mode:

  One of `"create"`, `"append"`, `"replace"`, `"create_append"` (error
  if the schema is not compatible or append otherwise), or `"default"`
  (use the `adbc.ingest.mode` argument of
  [`adbc_statement_init()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_statement_init.md)).
  The default is `"default"`.

- temporary:

  Use TRUE to create a table as a temporary table.

## Value

- `read_adbc()`: A
  [nanoarrow_array_stream](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array_stream.html)

- `execute_adbc()`: `db_or_con`, invisibly.

- `write_adbc()`: `tbl`, invisibly.

## Examples

``` r
# On a database, connections are opened and closed
db <- adbc_database_init(adbc_driver_log())
#> LogDatabaseNew()
#> LogDatabaseInit()
try(read_adbc(db, "some sql"))
#> LogConnectionNew()
#> LogConnectionInit()
#> LogStatementNew()
#> LogStatementSetSqlQuery()
#> LogStatementRelease()
#> LogConnectionRelease()
#> Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
try(execute_adbc(db, "some sql"))
#> LogConnectionNew()
#> LogConnectionInit()
#> LogStatementNew()
#> LogStatementSetSqlQuery()
#> LogStatementRelease()
#> LogConnectionRelease()
#> Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
try(write_adbc(mtcars, db, "some_table"))
#> LogConnectionNew()
#> LogConnectionInit()
#> LogStatementNew()
#> LogStatementSetOption()
#> LogStatementBindStream()
#> LogStatementRelease()
#> LogConnectionRelease()
#> Error in adbc_statement_bind_stream(stmt, tbl) : NOT_IMPLEMENTED

# Also works on a connection
con <- adbc_connection_init(db)
#> LogConnectionNew()
#> LogConnectionInit()
try(read_adbc(con, "some sql"))
#> LogStatementNew()
#> LogStatementSetSqlQuery()
#> LogStatementRelease()
#> Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
try(execute_adbc(con, "some sql"))
#> LogStatementNew()
#> LogStatementSetSqlQuery()
#> LogStatementRelease()
#> Error in adbc_statement_set_sql_query(stmt, query) : NOT_IMPLEMENTED
try(write_adbc(mtcars, con, "some_table"))
#> LogStatementNew()
#> LogStatementSetOption()
#> LogStatementBindStream()
#> LogStatementRelease()
#> Error in adbc_statement_bind_stream(stmt, tbl) : NOT_IMPLEMENTED
```
