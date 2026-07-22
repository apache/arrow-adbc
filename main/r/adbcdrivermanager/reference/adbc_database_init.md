# Databases

Databases

## Usage

``` r
adbc_database_init(driver = NULL, ...)

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
    [`adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.md).
    This includes drivers provided by R packages, such as
    [`adbcsqlite::adbcsqlite()`](https://arrow.apache.org/adbc/current/r/adbcsqlite/reference/adbcsqlite.html).

- ...:

  Driver-specific options. These are generally named values that are
  converted to strings.

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

## Driver inference and connection profiles

A driver can be selected by name, inferred from a URI, or loaded from a
connection profile:

    # Load a driver by name and pass it a URI
    adbc_database_init("postgresql", uri = "postgresql://localhost/database")

    # Infer "postgresql" from the URI scheme
    adbc_database_init(uri = "postgresql://localhost/database")

    # Load a named or absolute-path connection profile
    adbc_database_init(profile = "myprofile")

    # Equivalently, use a profile URI as the driver or URI
    adbc_database_init("profile://myprofile")
    adbc_database_init(uri = "profile://myprofile")

A minimal connection profile is a TOML file containing a driver and its
database options:

    profile_version = 1
    driver = "postgresql"

    [Options]
    uri = "postgresql://localhost/database"

Named profiles are located using the driver manager's standard profile
search paths. See the [ADBC connection profile
documentation](https://arrow.apache.org/adbc/current/format/connection_profiles.html)
for the file format, search locations, option precedence, and
environment variable substitution.

## Examples

``` r
adbc_database_init(adbc_driver_void())
#> <adbc_database at 0x556be5efaca0> 
#> List of 1
#>  $ driver:<adbc_driver_void> List of 4
#>   ..$ load_flags      : int 15
#>   ..$ driver_init_func:Class 'adbc_driver_init_func' <externalptr> 
#>   ..$ .child_count    : int 0
#>   ..$ version         : int 1001000
```
