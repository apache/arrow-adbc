# Log calls to another driver

Useful for debugging or ensuring that certain calls occur during
initialization and/or cleanup. The current logging output should not be
considered stable and may change in future releases.

## Usage

``` r
adbc_driver_log()
```

## Value

An object of class 'adbc_driver_log'

## Examples

``` r
drv <- adbc_driver_log()
db <- adbc_database_init(drv, key = "value")
#> LogDatabaseNew()
#> LogDatabaseSetOption()
#> LogDatabaseInit()
con <- adbc_connection_init(db, key = "value")
#> LogConnectionNew()
#> LogConnectionSetOption()
#> LogConnectionInit()
stmt <- adbc_statement_init(con, key = "value")
#> LogStatementNew()
#> LogStatementSetOption()
try(adbc_statement_execute_query(stmt))
#> LogStatementExecuteQuery()
#> Error in adbc_statement_execute_query(stmt) : NOT_IMPLEMENTED
adbc_statement_release(stmt)
#> LogStatementRelease()
adbc_connection_release(con)
#> LogConnectionRelease()
adbc_database_release(db)
#> LogDatabaseRelease()
```
