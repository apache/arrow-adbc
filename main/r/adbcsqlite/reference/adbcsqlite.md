# ADBC SQLite3 Driver

ADBC SQLite3 Driver

## Usage

``` r
adbcsqlite()

# S3 method for class 'adbcsqlite_driver_sqlite'
adbc_database_init(driver, ..., uri = ":memory:")

# S3 method for class 'adbcsqlite_database'
adbc_connection_init(database, ..., adbc.connection.autocommit = NULL)

# S3 method for class 'adbcsqlite_connection'
adbc_statement_init(
  connection,
  ...,
  adbc.ingest.target_table = NULL,
  adbc.ingest.target_catalog = NULL,
  adbc.ingest.mode = NULL,
  adbc.sqlite.query.batch_rows = NULL
)
```

## Arguments

- driver:

  The driver to use. This can be one of the following:

  - A non-missing `character(1)` containing a driver or manifest name, a
    relative or absolute path to a driver or manifest, or a URI. For a
    URI, the driver manager uses the URI scheme as the driver name and
    passes the URI to that driver. A `profile://` URI loads a connection
    profile.

  - `NULL`, which leaves driver selection to the driver manager. In this
    case, `...` must normally contain `uri` or `profile`; the driver is
    inferred from the URI or loaded from the connection profile.

  - An object that inherits from the `adbc_driver` class, such as one
    created by
    [`adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html).
    This includes drivers provided by R packages, such as
    `adbcsqlite::adbcsqlite()`.

- ...:

  Driver-specific options. These are generally named values that are
  converted to strings.

- uri:

  A URI to a database path or ":memory:" for an in-memory database.

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

- adbc.ingest.target_catalog:

  The catalog of the table for a bulk insert.

- adbc.ingest.mode:

  Whether to create (the default) or append.

- adbc.sqlite.query.batch_rows:

  The number of rows per batch to return.

## Value

An
[`adbcdrivermanager::adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.html)

## Examples

``` r
adbcsqlite()
#> <adbcsqlite_driver_sqlite> List of 4
#>  $ load_flags      : int 15
#>  $ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>  $ .child_count    : int 0
#>  $ version         : int 1001000
```
