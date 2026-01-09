# Driver search/load options

Options that indicate where to look for driver manifests. Manifests
(.toml files) can be installed at the system level, the user level, in
location(s) specified by the ADBC_DRIVER_PATH environment variable,
and/or in a conda environment. See the ADBC documentation for details
regarding the locations of the user and system paths on various
platforms.

## Usage

``` r
adbc_load_flags(
  search_env = TRUE,
  search_user = TRUE,
  search_system = TRUE,
  allow_relative_paths = TRUE
)
```

## Arguments

- search_env:

  Search for manifest files in the directories specified in the
  ADBC_DRIVER_PATH environment variable and (when installed with conda)
  in the conda environment.

- search_user:

  Search for manifest files in the designated directory for user ADBC
  driver installs.

- search_system:

  Search for manifest files in the designtaed directory for system ADBC
  driver installs.

- allow_relative_paths:

  Allow shared objects to be specified relative to the current working
  directory.

## Value

An integer flag value for use in
[`adbc_driver()`](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_driver_void.md)
