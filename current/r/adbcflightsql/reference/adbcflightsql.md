# ADBC FlightSQL Driver

ADBC FlightSQL Driver

## Usage

``` r
adbcflightsql()

# S3 method for class 'adbcflightsql_driver_flightsql'
adbc_database_init(driver, ..., uri = NULL)

# S3 method for class 'adbcflightsql_database'
adbc_connection_init(database, ..., adbc.connection.autocommit = NULL)

# S3 method for class 'adbcflightsql_connection'
adbc_statement_init(
  connection,
  ...,
  adbc.ingest.target_table = NULL,
  adbc.ingest.mode = NULL
)
```

## Arguments

- driver:

  An
  [`adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html).

- ...:

  Extra key/value options passed to the driver.

- uri:

  A URI to a database path (e.g.,
  `user[:password]@account/database[?param1=value1]`)

- database:

  An
  [adbc_database](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_database_init.html).

- adbc.connection.autocommit:

  Use FALSE to disable the default autocommit behaviour.

- connection:

  An
  [adbc_connection](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_connection_init.html)

- adbc.ingest.target_table:

  The name of the target table for a bulk insert.

- adbc.ingest.mode:

  Whether to create (the default) or append.

## Value

An
[`adbcdrivermanager::adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html)

## Examples

``` r
adbcflightsql()
#> <adbcflightsql_driver_flightsql> List of 4
#>  $ load_flags      : int 15
#>  $ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>  $ .child_count    : int 0
#>  $ version         : int 1001000
```
