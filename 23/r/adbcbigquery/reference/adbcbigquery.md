# ADBC BigQuery Driver

ADBC BigQuery Driver

## Usage

``` r
adbcbigquery()

# S3 method for class 'adbcbigquery_driver_bigquery'
adbc_database_init(driver, ..., token = NULL)

# S3 method for class 'adbcbigquery_database'
adbc_connection_init(database, ...)

# S3 method for class 'adbcbigquery_connection'
adbc_statement_init(connection, ...)
```

## Arguments

- driver:

  An
  [`adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html).

- ...:

  Extra key/value options passed to the driver.

- token:

  A token obtained from `bigrquery::bq_token()` or
  `gargle::token_fetch()`. This is the easiest way to authenticate.

- database:

  An
  [adbc_database](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_database_init.html).

- connection:

  An
  [adbc_connection](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_connection_init.html)

## Value

An
[`adbcdrivermanager::adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html)

## Examples

``` r
adbcbigquery()
#> <adbcbigquery_driver_bigquery> List of 4
#>  $ load_flags      : int 15
#>  $ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>  $ .child_count    : int 0
#>  $ version         : int 1001000
```
