
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

# adbcsnowflake

<!-- badges: start -->
<!-- badges: end -->

The goal of adbcsnowflake is to provide a low-level developer-facing
interface to the Arrow Database Connectivity (ADBC) Snowflake driver.

## Installation

You can install the development version of adbcsnowflake from
[GitHub](https://github.com/) with:

``` r
# install.packages("pak")
pak::pak("apache/arrow-adbc/r/adbcsnowflake")
```

ADBC drivers for R use a relatively new feature of pkgbuild to enable
installation from GitHub via pak. Depending on when you installed pak,
you may need to update its internal version of pkgbuild.

``` r
install.packages("pkgbuild", pak:::private_lib_dir())
pak::cache_clean()
```

## Example

This is a basic example which shows you how to solve a common problem. For examples
of `uri` values to use as a connection value, see the
[Snowflake driver documentation](https://arrow.apache.org/adbc/current/driver/snowflake.html#uri-format). Single sign-on browser-based authentication is also supported.

``` r
library(adbcdrivermanager)

# Use the driver manager to connect to a database. This example URI is
# <user>:<pass>@wt78143.<aws region>.aws/SNOWFLAKE_SAMPLE_DATA/TPCH_SF1?role=ACCOUNTADMIN
uri <- Sys.getenv("ADBC_SNOWFLAKE_TEST_URI")
db <- adbc_database_init(adbcsnowflake::adbcsnowflake(), uri = uri)
con <- adbc_connection_init(db)

con |>
  read_adbc("SELECT * FROM REGION ORDER BY R_REGIONKEY") |>
  tibble::as_tibble()
#> # A tibble: 5 × 3
#>   R_REGIONKEY R_NAME      R_COMMENT
#>         <dbl> <chr>       <chr>
#> 1           0 AFRICA      "lar deposits. blithely final packages cajole. regula…
#> 2           1 AMERICA     "hs use ironic, even requests. s"
#> 3           2 ASIA        "ges. thinly even pinto beans ca"
#> 4           3 EUROPE      "ly final courts cajole furiously final excuse"
#> 5           4 MIDDLE EAST "uickly special accounts cajole carefully blithely cl…
```

``` r
# Clean up
adbc_connection_release(con)
adbc_database_release(db)
```
