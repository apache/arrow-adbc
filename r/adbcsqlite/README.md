
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
adbc_database_init(adbcsqlite::adbcsqlite(), uri = ":memory:")
#> <adbcsqlite_database> <pointer: 0x12c7a1340> List of 2
#>  $ driver :<adbcsqlite_driver_sqlite> List of 1
#>   ..$ driver_init_func:Class 'adbc_driver_init_func' <externalptr>
#>  $ options:List of 1
#>   ..$ uri: chr ":memory:"
```
