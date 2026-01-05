# ADBC PostgreSQL Driver

ADBC PostgreSQL Driver

## Usage

``` r
adbcpostgresql()

# S3 method for class 'adbcpostgresql_driver_postgresql'
adbc_database_init(driver, ..., uri)

# S3 method for class 'adbcpostgresql_database'
adbc_connection_init(database, ..., adbc.connection.autocommit = NULL)

# S3 method for class 'adbcpostgresql_connection'
adbc_statement_init(
  connection,
  ...,
  adbc.ingest.target_table = NULL,
  adbc.ingest.target_db_schema = NULL,
  adbc.ingest.mode = NULL
)
```

## Arguments

- driver:

  An
  [`adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html).

- ...:

  Driver-specific options. For the default method, these are named
  values that are converted to strings.

- uri:

  A URI to a database path (e.g.,
  `postgresql://localhost:1234/postgres?user=user&password=password`)

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

- adbc.ingest.target_db_schema:

  The schema of the table for a bulk insert.

- adbc.ingest.mode:

  Whether to create (the default) or append.

## Value

An
[`adbcdrivermanager::adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html)

## Examples

``` r
adbcpostgresql()
#> <adbcpostgresql_driver_postgresql> List of 4
#>  $ load_flags      : int 15
#>  $ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>  $ .child_count    : int 0
#>  $ version         : int 1001000
```
