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

# ADBC Changelog

## ADBC Libraries 0.1.0 (2022-12-25)

### Fix

- **python**: make package names consistent (#258)
- **c/driver_manager**: accept connection options pre-Init (#230)
- **c/driver_manager,c/driver/postgres**: fix version inference from Git tags (#184)
- **c/driver/postgres**: fix duplicate symbols; add note about PKG_CONFIG_PATH (#169)
- **c/driver/postgres**: fix wheel builds (#161)
- **c/validation**: validate metadata more fully (#142)
- **c/validation**: free schema in partitioning test (#141)
- **c/validation**: cast to avoid MSVC warning (#135)

### Feat

- **c/driver_manager**: allow Arrow data as parameters in DBAPI layer (#245)
- **c/driver/postgres,c/driver/sqlite**: add pkg-config/CMake definitions (#231)
- **c/driver/sqlite**: add Python SQLite driver bindings (#201)
- **c/driver/sqlite**: port SQLite driver to nanoarrow (#196)
- **c/driver_manager**: expose ADBC functionality in DBAPI layer (#143)
- **c/driver_manager**: don't require ConnectionGetInfo (#150)

### Refactor

- **python**: allow overriding package version (#236)
- **c**: build Googletest if needed (#199)
- **c/driver_manager**: remove unnecessary libarrow dependency (#194)
- **c**: derive version components from base version (#178)
- **java/driver/jdbc**: use upstream JDBC utilities (#167)
- **c/validation**: split out test utilities (#151)

## ADBC Libraries 0.2.0 (2023-02-08)

### Fix

- **go/adbc/driver/flightsql**: deal with catalogless schemas (#422)
- **dev/release**: correct the name of the Go tag (#415)
- **go/adbc/driver/flightsql**: guard against inconsistent schemas (#409)
- **go/adbc/driver/driver/flightsql**: use libc allocator (#407)
- **ci**: make sure Conda packages are properly noarch (#399)
- **ci**: don't fail Anaconda upload when packages already exist (#398)
- **go/adbc/sqldriver**: allow equals signs in DSN values (#382)
- **c/driver/flightsql**: set GOARCH appropriately on macOS (#374)
- **ci**: don't make jobs depend on each other (#372)
- **go/adbc/driver/flightsql**: bind ExecuteUpdate, BindStream (#367)
- **go/adbc/driver/flightsql**: heap-allocate Go handles (#369)
- **go/adbc/driver/flightsql**: implement RecordReader.Err (#363)
- **go/adbc/flightsql**: enable Prepare (#362)
- **go/adbc/driver/flightsql**: connect to URI specified in FlightEndpoint (#361)
- **go/adbc/driver/flightsql**: cnxn should implement PostInitOptions (#357)
- **ci**: revert GEM_HOME for RubyGems install test (#351)
- **dev/release**: Update install location for RubyGems and Bundler (#345)
- **python**: add driver -> driver manager dependency (#312)
- **c/driver/postgresql**: define ntohll etc for macOS 10.9 (#305)

### Feat

- **go/sqldriver**: implement database/sql/driver.RowsColumnTypeDatabaseTypeName (#392)
- **go/sqldriver**: convert Arrow times and dates to Golang time.Time (#393)
- **go/adbc/driver/flightsql**: bump max incoming message size (#402)
- **go/adbc/driver/flightsql**: add timeout option handling (#390)
- **go/adbc/driver/flightsql**: implement GetObjects (#383)
- **go/adbc/driver/flightsql**: add auth and generic header options (#387)
- **go/adbc/driver/flightsql**: change parallelization of DoGet (#386)
- **go/adbc/driver/flightsql,go/adbc/sqldriver**: small improvements (#384)
- **go/adbc/driver/flightsql**: implement more connection options (#381)
- **python/adbc_driver_manager**: add more sanity checking (#370)
- **go/adbc/driver/pkg/cmake**: cmake build for Go shared library drivers (#356)
- **python**: add Flight SQL driver using Go library (#355)
- **go/adbc/driver/flightsql**: Build C shared lib for flightsql adbc driver (#347)
- **c/driver/postgresql**: expand type support (#352)
- **c/driver/postgresql**: add VARCHAR type support
- **go/adbc/driver/flightsql**: Native Golang ADBC Flight SQL driver (#322)

## ADBC Libraries 0.3.0 (2023-03-16)

### Fix

- **ci**: use conda git in verification (#518)
- **python/adbc_driver_manager**: properly map error codes (#510)
- **go/adbc/driver/flightsql**: properly map error codes (#509)
- **python/adbc_driver_manager**: expose set_options (#495)
- **go/adbc/driver/flightsql**: send headers in statement close (#494)
- **go/adbc/driver/flightsql**: fix stream timeout interceptor (#490)
- **go/adbc/driver/flightsql**: don't require GetSqlInfo support (#485)
- **c/driver/sqlite**: fix nullability of GetInfo schema (#457)
- **python/adbc_driver_manager**: fix Cursor.adbc_read_partition (#452)
- **r/adbcdrivermanager**: Check that bootstrap.R exists before trying to run it (#437)

### Feat

- **python**: add enums for common options (#513)
- **go/adbc/driver/flightsql**: support domain sockets (#516)
- **python/adbc_driver_manager**: add __del__ for resources (#498)
- **go/adbc/driver/flightsql**: add user-agent for driver (#497)
- **go/adbc/sqldriver**: add simple FlightSQL database/sql driver wrapper (#480)
- **java/driver/jdbc**: expose constraints in GetObjects (#474)
- **r/adbcsqlite**: Package SQLite driver for R (#463)
- **go/adbc/driver/flightsql**: add transaction and substrait support (#467)
- **r**: Add R Driver Manager (#365)
