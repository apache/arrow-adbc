
<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->
<!-- README.md is generated from README.Rmd. Please edit that file -->

# adbcsqlite

<!-- badges: start -->
<!-- badges: end -->

The goal of adbcdrivermanager is to provide a low-level developer-facing
interface to the Arrow Database Connectivity (ADBC) SQLite driver.

## Installation

You can install the development version of adbcsqlite from
[GitHub](https://github.com/) with:

``` r
# install.packages("remotes")
remotes::install_github("apache/arrow-adbc/r/adbcsqlite")
```

## Example

This is a basic example which shows you how to solve a common problem:

``` r
library(adbcdrivermanager)

# Use the driver manager to connect to a database
db <- adbc_database_init(adbcsqlite::adbcsqlite(), uri = ":memory:")
con <- adbc_connection_init(db)

# Write a table
flights <- nycflights13::flights
# (timestamp not supported yet)
flights$time_hour <- NULL

stmt <- adbc_statement_init(con, adbc.ingest.target_table = "flights")
adbc_statement_bind(stmt, flights)
adbc_statement_execute_query(stmt)
#> <nanoarrow_array_stream[invalid pointer]>
adbc_statement_release(stmt)

# Query it
stmt <- adbc_statement_init(con)
adbc_statement_set_sql_query(stmt, "SELECT * from flights")
result <- tibble::as_tibble(adbc_statement_execute_query(stmt))
adbc_statement_release(stmt)

result
#> # A tibble: 336,776 × 18
#>     year month   day dep_time sched_de…¹ dep_d…² arr_t…³ sched…⁴ arr_d…⁵ carrier
#>    <dbl> <dbl> <dbl>    <dbl>      <dbl>   <dbl>   <dbl>   <dbl>   <dbl> <chr>
#>  1  2013     1     1      517        515       2     830     819      11 UA
#>  2  2013     1     1      533        529       4     850     830      20 UA
#>  3  2013     1     1      542        540       2     923     850      33 AA
#>  4  2013     1     1      544        545      -1    1004    1022     -18 B6
#>  5  2013     1     1      554        600      -6     812     837     -25 DL
#>  6  2013     1     1      554        558      -4     740     728      12 UA
#>  7  2013     1     1      555        600      -5     913     854      19 B6
#>  8  2013     1     1      557        600      -3     709     723     -14 EV
#>  9  2013     1     1      557        600      -3     838     846      -8 B6
#> 10  2013     1     1      558        600      -2     753     745       8 AA
#> # … with 336,766 more rows, 8 more variables: flight <dbl>, tailnum <chr>,
#> #   origin <chr>, dest <chr>, air_time <dbl>, distance <dbl>, hour <dbl>,
#> #   minute <dbl>, and abbreviated variable names ¹​sched_dep_time, ²​dep_delay,
#> #   ³​arr_time, ⁴​sched_arr_time, ⁵​arr_delay

# Clean up
adbc_connection_release(con)
adbc_database_release(db)
```
