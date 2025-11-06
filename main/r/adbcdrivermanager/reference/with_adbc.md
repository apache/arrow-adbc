# Cleanup helpers

Managing the lifecycle of databases, connections, and statements can be
complex and error-prone. The R objects that wrap the underlying ADBC
pointers will perform cleanup in the correct order if you rely on
garbage collection (i.e., do nothing and let the objects go out of
scope); however it is good practice to explicitly clean up these
objects. These helpers are designed to make explicit and predictable
cleanup easy to accomplish.

## Usage

``` r
with_adbc(x, code)

local_adbc(x, .local_envir = parent.frame())
```

## Arguments

- x:

  An ADBC database, ADBC connection, ADBC statement, or
  nanoarrow_array_stream returned from calls to an ADBC function.

- code:

  Code to execute before cleaning up the input.

- .local_envir:

  The execution environment whose scope should be tied to the input.

## Value

- `with_adbc()` returns the result of `code`

- `local_adbc()` returns the input, invisibly.

## Details

Note that you can use
[`adbc_connection_join()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_connection_join.md)
and
[`adbc_statement_join()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_connection_join.md)
to tie the lifecycle of the parent object to that of the child object.
These functions mark any previous references to the parent object as
released so you can still use local and with helpers to manage the
parent object before it is joined. Use `stream_join_parent = TRUE` in
[`adbc_statement_execute_query()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_statement_set_sql_query.md)
to tie the lifecycle of a statement to the output stream.

## Examples

``` r
# Using with_adbc():
with_adbc(db <- adbc_database_init(adbc_driver_void()), {
  with_adbc(con <- adbc_connection_init(db), {
    with_adbc(stmt <- adbc_statement_init(con), {
      # adbc_statement_set_sql_query(stmt, "SELECT * FROM foofy")
      # adbc_statement_execute_query(stmt)
      "some result"
    })
  })
})
#> [1] "some result"

# Using local_adbc_*() (works best within a function, test, or local())
local({
  db <- local_adbc(adbc_database_init(adbc_driver_void()))
  con <- local_adbc(adbc_connection_init(db))
  stmt <- local_adbc(adbc_statement_init(con))
  # adbc_statement_set_sql_query(stmt, "SELECT * FROM foofy")
  # adbc_statement_execute_query(stmt)
  "some result"
})
#> [1] "some result"
```
