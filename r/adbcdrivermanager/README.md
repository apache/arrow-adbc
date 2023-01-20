
<!-- README.md is generated from README.Rmd. Please edit that file -->

# adbcdrivermanager

<!-- badges: start -->
<!-- badges: end -->

The goal of adbcdrivermanager is to provide a low-level developer-facing
interface to Arrow Database Connectivity for the purposes of driver
development and testing.

## Installation

You can install the development version of adbcdrivermanager from
[GitHub](https://github.com/) with:

``` r
# install.packages("remotes")
remotes::install_github("apache/arrow-adbc/r/adbcdrivermanager")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(adbcdrivermanager)
db <- adbcdrivermanager:::adbc_database_init(adbcdrivermanager:::adbc_driver_void())
adbcdrivermanager:::adbc_database_release(db)
```
