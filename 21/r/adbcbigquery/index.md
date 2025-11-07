# adbcbigquery

The goal of adbcbigquery is to provide a low-level developer-facing
interface to the Arrow Database Connectivity (ADBC) BigQuery driver.

## Installation

You can install the released version of adbcbigquery from
[R-multiverse](https://community.r-multiverse.org/) with:

``` r

install.packages("adbcbigquery", repos = "https://community.r-multiverse.org")
```

You can install the development version of adbcbigquery from
[GitHub](https://github.com/) with:

``` r

# install.packages("pak")
pak::pak("apache/arrow-adbc/r/adbcbigquery")
```

## Example

This is a basic example which shows you how to solve a common problem.

``` r

library(adbcdrivermanager)

# Use the driver manager to connect to a database
db <- adbc_database_init(
  adbcbigquery::adbcbigquery(),
  token = bigrquery::bq_token(),
  "adbc.bigquery.sql.project_id" = Sys.getenv("ADBC_BIGQUERY_TEST_PROJECT_ID")
)
con <- adbc_connection_init(db)

con |>
  read_adbc(
    "SELECT zipcode, latitude, longitude
      FROM `bigquery-public-data.utility_us.zipcode_area` LIMIT 10"
  ) |>
  tibble::as_tibble()
#> # A tibble: 10 × 3
#>    zipcode latitude longitude
#>    <chr>      <dbl>     <dbl>
#>  1 96950       15.2     146.
#>  2 96952       15.0     146.
#>  3 96951       14.2     145.
#>  4 96910       13.5     145.
#>  5 96929       13.6     145.
#>  6 96921       13.5     145.
#>  7 96913       13.5     145.
#>  8 96932       13.5     145.
#>  9 50012       42.0     -93.6
#> 10 52352       42.3     -91.8
```
