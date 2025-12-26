# Databases

Databases

## Usage

``` r
adbc_database_init(driver, ...)

adbc_database_init_default(driver, options = NULL, subclass = character())

adbc_database_release(database)

adbc_database_set_options(database, options)

adbc_database_get_option(database, option)

adbc_database_get_option_bytes(database, option)

adbc_database_get_option_int(database, option)

adbc_database_get_option_double(database, option)
```

## Arguments

- driver:

  An
  [`adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.md).

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

- database:

  An adbc_database.

- option:

  A specific option name

## Value

An object of class adbc_database

## Examples

``` r
adbc_database_init(adbc_driver_void())
#> <adbc_database at 0x55dc547ba980> 
#> List of 1
#>  $ driver:<adbc_driver_void> List of 4
#>   ..$ load_flags      : int 15
#>   ..$ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>   ..$ .child_count    : int 0
#>   ..$ version         : int 1001000
```
