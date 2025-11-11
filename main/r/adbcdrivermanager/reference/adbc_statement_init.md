# Statements

Statements

## Usage

``` r
adbc_statement_init(connection, ...)

adbc_statement_init_default(connection, options = NULL, subclass = character())

adbc_statement_release(statement)

adbc_statement_set_options(statement, options)

adbc_statement_get_option(statement, option)

adbc_statement_get_option_bytes(statement, option)

adbc_statement_get_option_int(statement, option)

adbc_statement_get_option_double(statement, option)
```

## Arguments

- connection:

  An
  [adbc_connection](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_connection_init.md)

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

- statement:

  An adbc_statement

- option:

  A specific option name

## Value

An object of class 'adbc_statement'

## Examples

``` r
db <- adbc_database_init(adbc_driver_void())
con <- adbc_connection_init(db)
adbc_statement_init(con)
#> <adbc_statement at 0x560da51abed0> 
#> List of 1
#>  $ connection:<adbc_connection at 0x560da51ed700> 
#> List of 1
#>   ..$ database:<adbc_database at 0x560da50adfd0> 
#> List of 1
#>   .. ..$ driver:<adbc_driver_void> List of 4
#>   .. .. ..$ load_flags      : int 15
#>   .. .. ..$ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>   .. .. ..$ .child_count    : int 0
#>   .. .. ..$ version         : int 1001000
```
