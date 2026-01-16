# Connections

Connections

## Usage

``` r
adbc_connection_init(database, ...)

adbc_connection_init_default(database, options = NULL, subclass = character())

adbc_connection_release(connection)

adbc_connection_set_options(connection, options)

adbc_connection_get_option(connection, option)

adbc_connection_get_option_bytes(connection, option)

adbc_connection_get_option_int(connection, option)

adbc_connection_get_option_double(connection, option)
```

## Arguments

- database:

  An
  [adbc_database](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_database_init.md).

- ...:

  Driver-specific options. For the default method, these are named
  values that are converted to strings.

- options:

  A named [`character()`](https://rdrr.io/r/base/character.html) or
  [`list()`](https://rdrr.io/r/base/list.html) whose values are
  converted to strings.

- subclass:

  An extended class for an object so that drivers can specify
  finer-grained control over behaviour at the R level.

- connection:

  An adbc_connection

- option:

  A specific option name

## Value

An object of class 'adbc_connection'

## Examples

``` r
db <- adbc_database_init(adbc_driver_void())
adbc_connection_init(db)
#> <adbc_connection at 0x55f925cae7e0> 
#> List of 1
#>  $ database:<adbc_database at 0x55f91fcabf10> 
#> List of 1
#>   ..$ driver:<adbc_driver_void> List of 4
#>   .. ..$ load_flags      : int 15
#>   .. ..$ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>   .. ..$ .child_count    : int 0
#>   .. ..$ version         : int 1001000
```
