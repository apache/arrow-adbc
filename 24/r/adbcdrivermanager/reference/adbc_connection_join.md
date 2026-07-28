# Join the lifecycle of a unique parent to its child

It is occasionally useful to return a connection, statement, or stream
from a function that was created from a unique parent. These helpers tie
the lifecycle of a unique parent object to its child such that the
parent object is released predictably and immediately after the child.
These functions will invalidate all references to the previous R object.

## Usage

``` r
adbc_connection_join(connection, database)

adbc_statement_join(statement, connection)
```

## Arguments

- connection:

  A connection created with
  [`adbc_connection_init()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_connection_init.md)

- database:

  A database created with
  [`adbc_database_init()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_database_init.md)

- statement:

  A statement created with
  [`adbc_statement_init()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_statement_init.md)

## Value

The input, invisibly.

## Examples

``` r
# Use local_adbc to ensure prompt cleanup on error;
# use join functions to return a single object that manages
# the lifecycle of all three.
stmt <- local({
  db <- local_adbc(adbc_database_init(adbc_driver_log()))

  con <- local_adbc(adbc_connection_init(db))
  adbc_connection_join(con, db)

  stmt <- local_adbc(adbc_statement_init(con))
  adbc_statement_join(stmt, con)

  adbc_xptr_move(stmt)
})
#> LogDatabaseNew()
#> LogDatabaseInit()
#> LogConnectionNew()
#> LogConnectionInit()
#> LogStatementNew()

# Everything is released immediately when the last object is released
adbc_statement_release(stmt)
#> LogStatementRelease()
#> LogConnectionRelease()
#> LogDatabaseRelease()
```
