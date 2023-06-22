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

## ADBC Libraries 0.4.0 (2023-05-08)

### Fix

- **ruby**: Free an imported reader in Statement#execute explicitly (#665)
- **go/adbc/driver/snowflake**: Skip shared dbs that have no data or we can't access (#656)
- **c/validation**: correct indexing in TestMetadataGetObjectsTables (#649)
- **c/driver**: hide symbols when buildling Go Flight SQL and Snowflake drivers (#640)
- **go/adbc/sqldriver**: do not swallow array.RecordReader error (#641)
- **go/adbc/driver/snowflake**: some more cleanup (#637)
- **c/driver/postgresql**: properly handle NULLs (#626)
- **java**: require supplying BufferAllocator to create drivers (#622)
- **go/adbc/driver/flightsql**: use updated authorization header from server (#594)
- **c/driver/sqlite,c/validation**: Ensure float/double values are not truncated on bind or select (#585)
- **java/driver/jdbc**: check for existence when getting table schema (#567)
- **java/driver/jdbc**: clean up buffer leaks (#533)
- **python/adbc_driver_manager**: fix uncaught exception in __del__ (#556)

### Feat

- **r/adbcsnowflake**: Package Snowflake driver for R (#638)
- **c/driver/postgresql**: implement GetTableSchema (#577)
- **python/adbc_driver_snowflake**: package the Snowflake driver (#633)
- **go/adbc/driver**: Adbc Driver for Snowflake (#586)
- **glib**: add gadbc_connection_get_objects() (#617)
- **java/driver/jdbc**: support catalogPattern in getObjects (#613)
- **java/driver/jdbc**: create AdbcDatabase from javax.sql.DataSource (#607)
- **glib**: add support for no AdbcError error case (#604)
- **ruby**: add support for statement.ingest("table", table) (#601)
- **glib**: add gadbc_connection_set_isolation_level() (#590)
- **glib**: add gadbc_connection_set_read_only() (#589)
- **java/driver/flight-sql**: allow passing BufferAllocator (#564)
- **glib**: add transaction related connection bindings (#579)
- **r/adbdpostgresql**: Package postgresql driver for R (#511)
- **glib**: add gadbc_connection_get_table_schema() (#576)
- **glib**: add gadbc_connection_get_info() (#571)
- **glib**: add gadbc_connection_get_table_types() (#560)
- **python/adbc_driver_manager**: expose StatementGetParameterSchema (#555)
- **glib**: add gadbc_statement_bind_stream() (#536)
- **docs**: maintain relative URL when switching versions (#531)
- **rpm**: add adbc-driver-flightsql (#526)
- **glib**: add gadbc_statement_bind() and ingest related bindings (#528)
- **deb**: add libadbc-driver-flightsql (#521)

### Refactor

- **c**: fix some build warnings (#654)
- **c/driver/postgresql**: Factor out COPY reader and test it independently (#636)
- **c/driver/postgresql**: Remove utils duplication (#628)
- **c/driver/common**: Variadic arguments for StringBuilderAppend (#587)
- **c**: merge CMake projects (#597)
- **c/driver/postgresql**: Factor out Postgres type abstraction and test it independently of the driver (#573)
- **c/driver/shared**: created shared util library for drivers (#582)

## ADBC Libraries 0.5.0 (2023-06-15)

### Feat

- **c/driver/postgresql**: Support INT16 Postgres Ingest (#800)
- **python/adbc_driver_manager**: add autocommit, executescript (#778)
- **c/driver/postgresql,java**: ensure time/date type support (#774)
- **c/driver/postgresql**: Implement Foreign Key information for GetObjects (#757)
- **c/driver/postgresql**: add timestamp types support (#758)
- **c/driver/postgresql**: Implement PRIMARY KEY in GetObjects ALL depth (#725)
- **csharp**: adding C# functionality (#697)
- **go/adbc/pkg**: catch panics at interface boundary (#730)
- **java/driver/jdbc**: add hooks for JDBC type system mapping (#722)
- **c/driver/postgresql**: Implement GetObjects for columns (#723)
- **c/driver/postgresql**: Implement GetObjects for tables (#712)
- **rust**: define the rust adbc api (#478)
- **c/driver/postgresql**: handle non-SELECT statements (#707)
- **c/driver/postgresql**: Implement GetObjectsDbSchemas for Postgres (#679)
- **r**: Add `read_adbc()`, `write_adbc()`, and `execute_adbc()` convenience functions (#706)
- **r**: Improve error communication (#703)
- **r**: Add scoping + lifecycle helpers (#693)
- **r**: Add driver logging utility (#694)
- **c/driver/postgresql**: implement GetObjectsSchema (#676)
- **go/adbc/driver/snowflake**: Update gosnowflake dep (#674)
- **c/driver/postgresql**: Implement Postgres Get table types (#668)
- **dev/release**: Retry on apt failure in the RC verification script (#672)
- **c/driver/postgresql**: Implement Postgres GetInfo (#658)

### Fix

- **go/adbc/pkg**: allow ConnectionSetOptions before Init (#789)
- **c/driver/sqlite**: support PRIMARY KEY constraint in GetObjects (#777)
- **c/driver/common**: Prevent UB in GetObjects with NULL argument (#786)
- **c**: Fix destructor mem leaks (#785)
- **java/driver/jdbc**: return timestamps as MICROSECOND always (#771)
- **go/adbc**: don't crash on duplicate column names (#766)
- **c/driver/postgresql**: Fix ASAN detected leaks (#768)
- **c/driver/sqlite**: Fix parameter binding when inferring types and when retrieving (#742)
- **python/adbc_driver_manager**: fix fetching queries with empty results (#744)
- **go/adbc/drivermgr**: go doesn't package symbolic links (#709)
- **r**: Don't save database/connection/statement options at the R level (#708)
- **go/adbc**: Update snowflake dep (#705)
- **c/driver/snowflake**: fix validation test failures (#677)
- **dev/release**: Fix BINARY_DIR prepare condition in the verify RC script (#670)
- **c/driver/postgresql**: Prevent SQL Injection in GetTableSchema (#657)

### Refactor

- **c/driver/postgresql**: More postgres test simplification (#784)
- **c/driver/postgresql**: Use AdbcGetInfoData structure (#769)
- **csharp**: Cleanup C API (#749)
- **go/adbc/driver/flightsql**: factor out server-based tests (#763)
- **java/driver/jdbc**: add JdbcQuirks for backend config (#748)
- **r/adbcdrivermanager**: Early exit (#740)
- **c/driver/postgresql**: Use Prepared Statement in Result Helper (#714)
- **c/driver/postgresql**: Postgres class helper for GetObjects (#711)
- **c**: Use ArrowArrayViewListChildOffset from nanoarrow (#696)
- **c/driver/postgresql**: implement InputIterator for ResultHelper (#683)
- **c**: Simplify CI testing for cpp (#610)

### Perf

- **go/adbc/driver/flightsql**: filter by schema in getObjectsTables (#726)

## ADBC Libraries 0.5.1 (2023-06-22)

### Feat

- **r**: Add FlightSQL driver wrapper (#835)
- **python/adbc_driver_flightsql**: add cookie middleware option to DatabaseOptions (#830)
- **go/adbc/driver/flightsql**: Add cookie middleware option (#825)
- **c/driver/postgresql**: Implement GetObjects with table_types argument (#799)
- **c/driver/postgresql**: Binary ingest (#808)
- **c/driver/postgresql**: Support float type (#807)

### Fix

- **go/adbc/driver/snowflake**: fix potential deadlock and error handling (#828)
- **csharp**: submodule not pulling correctly (#824)
- **go/adbc/driver/snowflake**: initialize Params, add DLL build (#820)
- **dev/release**: add missing duckdb dependency (#810)

### Refactor

- **csharp**: cleanup load of imported drivers (#818)
