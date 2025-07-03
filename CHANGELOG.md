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

## ADBC Libraries 0.6.0 (2023-08-23)

### Feat

- **python/adbc_driver_manager**: add fetch_record_batch (#989)
- **c/driver**: Date32 support (#948)
- **c/driver/postgresql**: Interval support (#908)
- **go/adbc/driver/flightsql**: add context to gRPC errors (#921)
- **c/driver/sqlite**: SQLite timestamp write support (#897)
- **c/driver/postgresql**: Handle NUMERIC type by converting to string (#883)
- **python/adbc_driver_postgresql**: add PostgreSQL options enum (#886)
- **c/driver/postgresql**: TimestampTz write (#868)
- **c/driver/postgresql**: Implement streaming/chunked output (#870)
- **c/driver/postgresql**: Timestamp write support (#861)
- **c/driver_manager,go/adbc,python**: trim down error messages (#866)
- **c/driver/postgresql**: Int8 support (#858)
- **c/driver/postgresql**: Better type error messages (#860)

### Fix

- **go/adbc/driver/flightsql**: Have GetTableSchema check for table name match instead of the first schema it receives (#980)
- **r**: Ensure that info_codes are coerced to integer (#986)
- **go/adbc/sqldriver**: fix handling of decimal types (#970)
- **c/driver/postgresql**: Fix segfault associated with uninitialized copy_reader_ (#964)
- **c/driver/sqlite**: add table types by default from arrow types (#955)
- **csharp**: include GetTableTypes and GetTableSchema call for .NET 4.7.2  (#950)
- **csharp**: include GetInfo and GetObjects call for .NET 4.7.2 (#945)
- **c/driver/sqlite**: Wrap bulk ingests in a single begin/commit txn (#910)
- **csharp**: fix C api to work under .NET 4.7.2 (#931)
- **python/adbc_driver_snowflake**: allow connecting without URI (#923)
- **go/adbc/pkg**: export Adbc* symbols on Windows (#916)
- **go/adbc/driver/snowflake**: handle non-arrow result sets (#909)
- **c/driver/sqlite**: fix escaping of sqlite TABLE CREATE columns (#906)
- **go/adbc/pkg**: follow CGO rules properly (#902)
- **go/adbc/driver/snowflake**: Fix integration tests by fixing timestamp handling (#889)
- **go/adbc/driver/snowflake**: fix failing integration tests (#888)
- **c/validation**: Fix ASAN-detected leak (#879)
- **go/adbc**: fix crash on map type (#854)
- **go/adbc/driver/snowflake**: handle result sets without Arrow data (#864)

### Perf

- **go/adbc/driver/snowflake**: Implement concurrency limit (#974)

### Refactor

- **c**: Vendor portable-snippets for overflow checks (#951)
- **c/driver/postgresql**: Use ArrowArrayViewGetIntervalUnsafe from nanoarrow (#957)
- **c/driver/postgresql**: Simplify current database querying (#880)

## ADBC Libraries 0.7.0 (2023-09-20)

### Feat

- **r**: Add quoting/escaping generics (#1083)
- **r**: Implement temporary table option in R driver manager (#1084)
- **python/adbc_driver_flightsql**: add adbc.flight.sql.client_option.authority to DatabaseOptions (#1069)
- **go/adbc/driver/snowflake**: improve XDBC support (#1034)
- **go/adbc/driver/flightsql**: add adbc.flight.sql.client_option.authority  (#1060)
- **c/driver**: support ingesting into temporary tables (#1057)
- **c/driver**: support target catalog/schema for ingestion (#1056)
- **go**: add basic driver logging (#1048)
- **c/driver/postgresql**: Support ingesting LARGE_STRING types (#1050)
- **c/driver/postgresql**: Duration support (#907)
- ADBC API revision 1.1.0 (#971)

### Fix

- **java/driver/flight-sql**: fix leak in InfoMetadataBuilder (#1070)
- **c/driver/postgresql**: Fix overflow in statement.cc (#1072)
- **r/adbcdrivermanager**: Ensure nullable arguments `adbc_connection_get_objects()` can be specified (#1032)
- **c/driver/sqlite**: Escape table name in sqlite GetTableSchema (#1036)
- **c/driver**: return NOT_FOUND for GetTableSchema (#1026)
- **c/driver_manager**: fix crash when error is null (#1029)
- **c/driver/postgresql**: suppress console spam (#1027)
- **c/driver/sqlite**: escape table names in INSERT, too (#1003)
- **go/adbc/driver/snowflake**: properly handle time fields (#1021)
- **r/adbcdrivermanager**: Make `adbc_xptr_is_valid()` return `FALSE` for external pointer to NULL (#1007)
- **go/adbc**: don't include NUL in error messages (#998)

### Refactor

- **c/driver/postgresql**: hardcode overflow checks (#1051)

## ADBC Libraries 0.8.0 (2023-11-03)

### Feat

- **c/driver/sqlite**: enable extension loading (#1162)
- **csharp**: Add support for SqlDecimal (#1241)
- **go/adbc/driver/snowflake**: enable passing private key for JWT via string and not file (#1207)
- **c/driver/sqlite**: Support binding dictionary-encoded string and binary types (#1224)
- **c/driver/sqlite**: Support BLOB in result sets for SQLite (#1223)
- **csharp/drivers/bigquery**: add BigQuery ADBC driver (#1192)
- **go/adbc/driver/snowflake**: support PEM decoding JWT private keys (#1199)
- **r/adbcdrivermanager**: Implement missing function mappings (#1206)
- **c/driver/postgresql**: Use COPY for writes (#1093)
- **c/driver/postgresql**: INSERT benchmark for postgres (#1189)
- **c/driver/postgresql**: Binary COPY Writer (#1181)
- **c/driver/postgresql**: INTERVAL COPY Writer (#1184)
- **c/driver/postgresql**: TIMESTAMP COPY Writer (#1185)
- **c/driver/postgresql**: DATE32 Support for COPY Writer (#1182)
- **c/driver/postgresql**: INT8 Support in COPY Writer (#1176)
- **c/driver/postgresql**: Floating point types for Copy Writer (#1170)
- **c/driver/postgresql**: String/Large String COPY Writers (#1172)
- **csharp**: Add ADO.NET client; tests for C# to interop with the Snowflake Go driver (#1031)
- **c/driver/postgresql,c/driver/sqlite**: Implement FOREIGN KEY constraints (#1099)
- **go/adbc/driver/flightsql**: log new connections (#1146)
- **c/driver/postgresql**: add integral COPY writers (#1130)
- **c/driver/postgresql**: Initial COPY Writer design (#1110)
- **c/driver/postgresql,c/driver/sqlite**: implement BOOL support in drivers (#1091)

### Fix

- **c/driver**: be explicit about columns in ingestion (#1238)
- **go/adbc/driver/flightsql**: take metadata lock for safety (#1228)
- **c/driver/sqlite**: Provide # of rows affected for non-SELECT statements instead of 0 (#1179)
- **r/adbcpostgresql**: Use libpq provided by Rtools for R 4.2 and R 4.3 (#1218)
- **r/adbcsqlite**: Fix incomplete cleanup in adbcsqlite tests (#1219)
- **c/driver/postgresql**: Allow ctest to run benchmark (#1203)
- **r/adbcsnowflake**: Add arrow as check dependency for adbcsnowflake (#1208)
- **r/adbcdrivermanager**: Improve handling of integer and character list inputs (#1205)
- **r**: Build with __USE_MINGW_ANSI_STDIO to enable use of lld in format strings (#1180)
- **c/driver/postgresql**: only clear schema option if needed (#1174)
- **c/driver/postgresql**: Support trailing semicolon(s) for queries inside COPY statements (#1171)
- **c/driver/common**: Object name matching handles shared prefix case correctly (#1168)
- **r/adbcdrivermanager**: Fix tests to avoid moving an external pointer with dependents (#1167)
- **r/adbcsnowflake**: Don't use test snowflake query that returns a decimal type (#1164)
- **r/adbcdrivermanager**: Add format method for adbc_xptr (#1165)
- **r/adbcdrivermanager**: Use ADBC_VERSION_1_1_0 to initialize drivers internally (#1163)
- **go/adbc/driver/snowflake**: add useHighPrecision option for decimal vs int64 (#1160)
- **c/driver/postgresql**: reset transaction after rollback (#1159)
- **go/adbc/driver/snowflake**: proper timezone for timestamp_ltz (#1155)
- **c/driver_manager**: Include cerrno in driver manager (#1137)
- **r/adbcdrivermanager**: Fix valgrind errors identified by CRAN 0.7.0 submission (#1136)
- **go/adbc/driver/snowflake**: default SetOption to StatusNotImplemented (#1120)
- **go/adbc/pkg**: support Windows in Makefile (#1114)
- **go/adbc/driver/snowflake**: prevent database options from getting overwritten (#1097)
- **python/adbc_driver_manager**: allow non-indexable sequences in executemany (#1094)

### Refactor

- **r**: Improve testing for ADBC 1.1 features in R bindings (#1214)
- **c/driver/postgresql**: Macro for benchmark return (#1202)
- **c/driver/postgresql**: Refactor COPY Writer NULL handling (#1175)
- **c/driver/postgresql**: Have Copy Writer manage its own buffer (#1148)
- **go/adbc/driver**: add driver framework (#1081)
- **c/driver/postgresql**: remove unnecessary destructor (#1134)
- **c/driver/postgresql**: refactor Handle class (#1132)

## ADBC Libraries 0.9.0 (2024-01-03)

### Fix

- **go/adbc/driver/snowflake**: Removing SQL injection to get table name with special character for getObjectsTables (#1338)
- **dev/release**: install openssl explicitly for R CMD check (#1427)
- **java/driver/jdbc**: fix connection leak in `JdbcDataSourceDatabase` constructor  (#1418)
- **c/driver/postgresql**: fix ingest with multiple batches (#1393)
- **c/driver/postgresql**: check for underflow (#1389)
- **c/driver/postgresql**: support catalog arg of GetTableSchema (#1387)
- **go/adbc/sqldriver**: Fix nil pointer panics for query parameters (#1342)
- **go/adbc/driver/snowflake**: Made GetObjects case insensitive (#1328)
- **csharp/src/Drivers/BigQuery**: Fix failure when returning multiple table schemas from BigQuery (#1336)
- **csharp/src/Drivers/BigQuery**: Fix failure when returning empty schema data from BigQuery (#1330)
- **go/adbc/driver/snowflake**: Fix race condition in record reader (#1297)
- **go/adbc/driver/snowflake**: fix XDBC support when using high precision (#1311)
- **csharp/src/Drivers/BigQuery**: Add JSON support (#1308)
- **glib**: Vala's vapi's name should be same as pkg-config package (#1298)
- **csharp/adbc**: Remove xunit as a dependency from Apache.Arrow.Adbc.dll (#1295)
- **csharp/client/adbcconnection**: reset AdbcStatement on dispose (#1289)
- **csharp/drivers/bigquery**: add back support for Struct and List arrays (#1282)
- **csharp**: fix timestamp case in AdbcStatement.GetValue (#1279)
- **go/adbc/driver/snowflake**: handling of integer values sent for NUMBER columns (#1267)
- **glib**: add missing "pkg-config --modversion arrow-glib" result check (#1266)
- **dev/release**: install missing protobuf for Python test (#1264)
- **r/adbcdrivermanager**: Ensure test driver includes null terminator when fetching string option (#1258)
- **r/adbcsqlite**: Allow sqlite driver to link against sqlite3 that does not contain sqlite3_load_extension() (#1259)

### Feat

- **c/driver/postgresql**: Support for writing DECIMAL types (#1288)
- **c/driver/postgresql**: set rows_affected appropriately (#1384)
- **r**: Reference count child objects (#1334)
- **python/adbc_driver_manager**: export handles through python Arrow Capsule interface (#1346)
- **csharp/Client**: Implement support for primary schema collections (#1317)
- **go/adbc/drivermgr**: Implement Remaining CGO Wrapper Methods that are Supported by SQLite Driver (#1304)
- **go/adbc/drivermgr**: Implement GetObjects for CGO Wrapper (#1290)
- **csharp**: Translate time to either TimeSpan or TimeOnly (#1293)
- **glib**: add Vala VAPI for GADBC (#1152)
- **csharp/test**: Add support for data without need for tables (#1287)
- **c/driver/postgresql**: Accept bulk ingest of dictionary-encoded strings/binary (#1275)
- **r/adbcdrivermanager**: Add support for setting option as logical (#1270)
- **go/adbc/driver/snowflake**: add support for ExecuteSchema (#1262)

### Perf

- **go/adbc/driver/snowflake**: GetObjects call is slow even when filters are provided (#1285)

### Refactor

- **r/adbcdrivermanager**: Use C++ base driver to implement test drivers (#1269)

## ADBC Libraries 0.10.0 (2024-02-18)

### Feat

- **java/driver/flight-sql**: implement getObjects (#1517)
- **go/adbc/driver/snowflake**: add '[ADBC]' to snowflake application name (#1525)
- **python/adbc_driver_manager**: handle KeyboardInterrupt (#1509)
- **csharp/src/Drivers/BigQuery**: add override for excluding table constraints (#1512)
- **java/driver/flight-sql**: add basic auth (#1487)
- **c/driver/postgresql**: Add enum type support (#1485)
- **go/adbc/driver/flightsql**: propagate cookies to sub-clients (#1497)
- **go/adbc/driver/snowflake**: improve bulk ingestion speed (#1456)
- **glib**: Add Apache Arrow GLib integration library (#1459)
- **go/adbc/driver/flightsql**: enable incremental queries (#1457)
- **go/adbc**: close database explicitly  (#1460)

### Refactor

- **c/validation**: split up large test file (#1541)
- **c/driver/postgresql**: update with C++17 conventions (#1540)
- **c/driver/postgresql**: No naked new in copy/reader.h (#1503)
- **c/driver/postgresql**: No naked new in copy writer (#1498)
- **c/driver/postgresql**: start C++17 (#1472)
- **csharp/test/Drivers/Interop/Snowflake**: Updated the metadata tests to work without the db name (#1352)
- **c/driver/postgresql**: Split postgres_copy_reader.h into reader/writer headers (#1432)

### Fix

- **python/adbc_driver_manager**: return 'real' reader in fetch_record_batch (#1530)
- **go/adbc/driver/flightsql**: use atomic for progress (#1520)
- **c/driver/postgresql**: add postgres type to cols created for numeric (#1516)
- **csharp/src/Drivers/BigQuery**: fix support for large results (#1507)
- **c/driver/postgresql**: fix numeric to str (#1502)
- **go/adbc/driver/snowflake**: Make SHOW WAREHOUSES test less flaky (#1494)
- **csharp/src/Drivers/BigQuery**: add support for scopes (#1482)
- **r/adbcpostgresql**: Link -lcrypt32 on Windows (#1471)
- **csharp/src/Drivers/BigQuery**: improved support for ARRAY columns (#1356)
- **java/driver/jdbc**: improve error messages thrown from JdbcDataSource connect failures (#1466)
- **ci**: remove invalid --version=14 clang-format argument (#1462)
- **r/adbcdrivermanager**: Use std::vector<uint8_t> instead of std::basic_string<uint8_t> (#1453)
- **dev/release**: remove gtest without prompting on Windows (#1439)
- **dev/release,glib**: set library path to run example (#1436)
- **dev/release,go**: ensure temporary directory removable (#1438)

## ADBC Libraries 0.11.0 (2024-03-28)

### Feat

- **go/adbc/driver/snowflake**: added table constraints implementation for GetObjects API (#1593)
- **dev/release**: add C# post-release script (#1595)
- **go/adbc/driver/flightsql**: support session options (#1597)
- **go/adbc/driver/flightsql**: support reuse-connection location (#1594)
- **go/adbc/driver/flightsql**: expose FlightInfo during polling (#1582)
- **go/adbc/driver/flightsql**: reflect gRPC status in vendor code (#1577)
- **python**: react to SIGINT in more places (#1579)
- **c/driver/common**: Add minimal C++ driver framework (#1539)

### Fix

- **ci**: Sanitize PR title (#1677)
- **go/adbc/driver/snowflake**: fix precision/scale in table schema (#1656)
- **python**: correct typechecking error (#1604)
- **go/adbc/drivermgr**: don't call potentially nil pointer (#1586)
- **csharp/src/Client/SchemaConverter**: add check for keys on precision and scale (#1566)
- **csharp/src/Drivers/BigQuery**: return null on GetValue exception (#1562)

### Refactor

- **c/driver_manager**: differentiate errors from driver manager (#1662)
- **c/driver/sqlite**: port to driver base (#1603)
- **go/adbc/driver**: driverbase implementation for connection (#1590)

## ADBC Libraries 12 (2024-05-09)

### Fix

- **csharp/src/Apache.Arrow.Adbc/C**: finalizer threw exception (#1842)
- **dev/release**: handle versioning scheme in binary verification (#1834)
- **csharp**: Change option translation to be case-sensitive (#1820)
- **csharp/src/Apache.Arrow.Adbc/C**: imported errors don't return a native error or SQL state (#1815)
- **csharp/src/Apache.Arrow.Adbc**: imported statements and databases don't allow options to be set (#1816)
- **csharp/src/Apache.Arrow.Adbc/C**: correctly handle null driver entries for imported drivers (#1812)
- **go/adbc/driver/snowflake**: handle empty result sets (#1805)
- **csharp**: an assortment of small fixes not worth individual pull requests (#1807)
- **go/adbc/driver/snowflake**: workaround snowflake metadata-only limitations (#1790)
- **csharp/src/Apache.Arrow.Adbc**: correct StandardSchemas.ColumnSchema data types (#1731)
- **csharp**: imported drivers have the potential for a lot of memory leaks (#1776)
- **go/adbc/driver/flightsql**: should use `ctx.Err().Error()` (#1769)
- **go/adbc/driver/snowflake**: handle quotes properly (#1738)
- **go/adbc/driver/snowflake**: comment format (#1768)
- **csharp/src/Apache.Arrow.Adbc**: Fix marshaling in three functions where it was broken (#1758)
- **csharp/src/Apache.Arrow.Adbc**: Add support to the C Exporter for converting exceptions into AdbcErrors (#1752)
- **format**: correct duplicated statistics names (#1736)
- **dev/release**: correct C# version bump regex (#1733)
- **r**: Ensure CXX_STD is set everywhere (#1706)
- **csharp**: Resolve memory leaks described by #1690 (#1695)

### Feat

- **go/adbc/driver/snowflake**: support parameter binding (#1808)
- **csharp**: imported objects should have call "release" when no longer in use (#1802)
- **rust**: add the driver exporter (#1756)
- **csharp**: enable nullable checks and resolve all warnings (#1792)
- **go/adbc/driver/flightsql**: support stateless prepared statements (#1796)
- **csharp**: Implement support for transactions, isolation level and read-only flag (#1784)
- **csharp/test**: implement DuckDb test fixture (#1781)
- **csharp**: Implement remaining functions in 1.0 spec (#1773)
- **csharp/src/Apache.Arrow.Adbc**: Cleanup use of List<T> in APIs and implementation (#1761)
- **csharp**: Update to test with net8.0 (#1771)
- **csharp/src/Apache.Arrow.Adbc**: Remove AdbcConnection.GetInfo(List<int>) (#1760)
- **glib**: add GADBCArrowConnection (#1754)
- **rust**: add complete FFI bindings (#1742)
- **glib**: Add garrow_connection_get_statistic_names() (#1748)
- **glib**: Add garrow_connection_get_statistics() (#1744)
- **c/driver/postgresql**: add money type and test intervals (#1741)
- **rust**: add public abstract API and dummy driver implementation (#1725)
- **csharp/src/Drivers**: introduce drivers for Apache systems built on Thrift (#1710)
- **format**: add info codes for supported capabilities (#1649)

## apache-arrow-adbc-12-rc3 (2024-05-09)

### Fix

- **csharp/src/Apache.Arrow.Adbc/C**: finalizer threw exception (#1842)
- **dev/release**: handle versioning scheme in binary verification (#1834)
- **csharp**: Change option translation to be case-sensitive (#1820)
- **csharp/src/Apache.Arrow.Adbc/C**: imported errors don't return a native error or SQL state (#1815)
- **csharp/src/Apache.Arrow.Adbc**: imported statements and databases don't allow options to be set (#1816)
- **csharp/src/Apache.Arrow.Adbc/C**: correctly handle null driver entries for imported drivers (#1812)
- **go/adbc/driver/snowflake**: handle empty result sets (#1805)
- **csharp**: an assortment of small fixes not worth individual pull requests (#1807)
- **go/adbc/driver/snowflake**: workaround snowflake metadata-only limitations (#1790)
- **csharp/src/Apache.Arrow.Adbc**: correct StandardSchemas.ColumnSchema data types (#1731)
- **csharp**: imported drivers have the potential for a lot of memory leaks (#1776)
- **go/adbc/driver/flightsql**: should use `ctx.Err().Error()` (#1769)
- **go/adbc/driver/snowflake**: handle quotes properly (#1738)
- **go/adbc/driver/snowflake**: comment format (#1768)
- **csharp/src/Apache.Arrow.Adbc**: Fix marshaling in three functions where it was broken (#1758)
- **csharp/src/Apache.Arrow.Adbc**: Add support to the C Exporter for converting exceptions into AdbcErrors (#1752)
- **format**: correct duplicated statistics names (#1736)
- **dev/release**: correct C# version bump regex (#1733)
- **r**: Ensure CXX_STD is set everywhere (#1706)
- **csharp**: Resolve memory leaks described by #1690 (#1695)

### Feat

- **go/adbc/driver/snowflake**: support parameter binding (#1808)
- **csharp**: imported objects should have call "release" when no longer in use (#1802)
- **rust**: add the driver exporter (#1756)
- **csharp**: enable nullable checks and resolve all warnings (#1792)
- **go/adbc/driver/flightsql**: support stateless prepared statements (#1796)
- **csharp**: Implement support for transactions, isolation level and read-only flag (#1784)
- **csharp/test**: implement DuckDb test fixture (#1781)
- **csharp**: Implement remaining functions in 1.0 spec (#1773)
- **csharp/src/Apache.Arrow.Adbc**: Cleanup use of List<T> in APIs and implementation (#1761)
- **csharp**: Update to test with net8.0 (#1771)
- **csharp/src/Apache.Arrow.Adbc**: Remove AdbcConnection.GetInfo(List<int>) (#1760)
- **glib**: add GADBCArrowConnection (#1754)
- **rust**: add complete FFI bindings (#1742)
- **glib**: Add garrow_connection_get_statistic_names() (#1748)
- **glib**: Add garrow_connection_get_statistics() (#1744)
- **c/driver/postgresql**: add money type and test intervals (#1741)
- **rust**: add public abstract API and dummy driver implementation (#1725)
- **csharp/src/Drivers**: introduce drivers for Apache systems built on Thrift (#1710)
- **format**: add info codes for supported capabilities (#1649)

## ADBC Libraries 13 (2024-07-01)

### Versions

- C/C++/GLib/Go/Python/Ruby: 1.1.0
- C#: 0.13.0
- Java: 0.13.0
- R: 0.13.0
- Rust: 0.13.0

### Fix

- **c/driver/sqlite**: Make SQLite driver C99 compliant (#1946)
- **go/adbc/pkg**: clean up potential sites where Go GC may be exposed (#1942)
- **c/driver/postgresql**: chunk large COPY payloads (#1937)
- **go/adbc/pkg**: guard against potential crash (#1938)
- **csharp/src/Drivers/Interop/Snowflake**: Swapping PreBuildEvent to DispatchToInnerBuilds (#1909)
- **csharp/src/Drivers/Apache/Spark**: fix parameter naming convention (#1895)
- **csharp/src/Apache.Arrow.Adbc/C**: GetObjects should preserve a null tableTypes parameter value (#1894)
- **go/adbc/driver/snowflake**: Records dropped on ingestion when empty batch is present (#1866)
- **csharp**: Fix packing process (#1862)
- **csharp/src/Drivers/Apache**: set the precision and scale correctly on Decimal128Type (#1858)

### Feat

- Meson Support for ADBC (#1904)
- **rust**: add integration tests and some improvements (#1883)
- **csharp/src/Drivers/Apache/Spark**: extend SQL type name parsing for all types (#1911)
- **csharp/src/Drivers/Apache**: improve type name handling for CHAR/VARCHAR/DECIMAL (#1896)
- **csharp/src/Drivers/Apache**: improve GetObjects metadata returned for columns (#1884)
- **rust**: add the driver manager (#1803)
- **csharp**: redefine C# APIs to prioritize full async support (#1865)
- **csharp/src/Drivers/Apache**: extend capability of GetInfo for Spark driver (#1863)
- **csharp/src/Drivers/Apache**: add implementation for AdbcStatement.SetOption on Spark driver (#1849)
- **csharp**: Move more options to be set centrally and enable TreatWarningsAsErrors (#1852)
- **csharp**: Initial changes for ADBC 1.1 in C# implementation (#1821)
- **csharp/src/Drivers/Apache/Spark**: implement async overrides for Spark driver (#1830)

## ADBC Libraries 14 (2024-08-30)

### Versions

- C/C++/GLib/Go/Python/Ruby: 1.2.0
- C#: 0.14.0
- Java: 0.14.0
- R: 0.14.0
- Rust: 0.14.0

### Feat

- **go/adbc/driver/snowflake**: Keep track of all files copied and skip empty files in bulk_ingestion (#2106)
- **dev/release**: add Rust release process (#2107)
- **go/adbc/driver/bigquery**: Implement GetObjects and get tests passing (#2044)
- **csharp/src/Client**: add support for parameterized execution (#2096)
- **c/driver/postgresql**: Support queries that bind parameters and return a result (#2065)
- **c/driver/postgresql**: Support JSON and JSONB types (#2072)
- **go/adbc/driver/bigquery**: add schema to reader for BigQuery (#2050)
- **c/driver/postgresql**: Implement consuming a PGresult via the copy reader (#2029)
- **csharp/src/Drivers/BigQuery**: add support for configurable query timeouts (#2043)
- **go/adbc/driver/snowflake**: use vectorized scanner for bulk ingest (#2025)
- **c**: Add BigQuery library to Meson build system (#1994)
- **c**: Add pkgconfig support to Meson build system (#1992)
- **c/driver/postgresql**: FIXED_SIZED_LIST Writer support (#1975)
- **go/adbc/driver**: add support for Google BigQuery (#1722)
- **c/driver/postgresql**: Implement LIST/LARGE_LIST Writer (#1962)
- **c/driver/postgresql**: Read/write support for TIME64[us] (#1960)
- **c/driver/postgresql**: UInt(8/16/32) Writer (#1961)

### Refactor

- **c/driver/framework**: Separate C/C++ conversions and error handling into minimal "base" framework (#2090)
- **c/driver/framework**: Remove fmt as required dependency of the driver framework (#2081)
- **c**: Updated include/install paths for adbc.h (#1965)
- **c/driver/postgresql**: Factory func for CopyWriter construction (#1998)
- **c**: Check MakeArray/Batch Error codes with macro (#1959)

### Fix

- **go/adbc/driver/snowflake**: Bump gosnowflake to fix context error (#2091)
- **c/driver/postgresql**: Fix ingest of streams with zero arrays (#2073)
- **csharp/src/Drivers/BigQuery**: update BigQuery test cases (#2048)
- **ci**: Pin r-lib actions as a workaround for latest action updates (#2051)
- **csharp/src/Drivers/BigQuery**: update BigQuery documents (#2047)
- **go/adbc/driver/snowflake**: split files properly after reaching targetSize on ingestion (#2026)
- **c/driver/postgresql**: Ensure schema ordering is consistent and respects case sensitivity of table names (#2028)
- **docs**: update broken link (#2016)
- **docs**: correct snowflake options for bulk ingest (#2004)
- **go/adbc/driver/flightsql**: propagate headers in GetObjects (#1996)
- **c/driver/postgresql**: Fix compiler warning on gcc14 (#1990)
- **r/adbcdrivermanager**: Ensure that class of object is checked before calling R_ExternalPtrAddrFn (#1989)
- **ci**: update website_build.sh for new versioning scheme (#1972)
- **dev/release**: update C# <VersionSuffix> tag (#1973)
- **c/vendor/nanoarrow**: Fix -Wreorder warning (#1966)

## ADBC Libraries 15 (2024-11-08)

### Versions

- C/C++/GLib/Go/Python/Ruby: 1.3.0
- C#: 0.15.0
- Java: 0.15.0
- R: 0.15.0
- Rust: 0.15.0

### Feat

- **c/driver/postgresql**: Enable basic connect/query workflow for Redshift (#2219)
- **rust/drivers/datafusion**: add support for bulk ingest (#2279)
- **csharp/src/Drivers/Apache**: convert Double to Float for Apache Spark on scalar conversion (#2296)
- **go/adbc/driver/snowflake**: update to the latest 1.12.0 gosnowflake driver (#2298)
- **csharp/src/Drivers/BigQuery**: support max stream count setting when creating read session (#2289)
- **rust/drivers**: adbc driver for datafusion (#2267)
- **go/adbc/driver/snowflake**: improve GetObjects performance and semantics (#2254)
- **c**: Implement ingestion and testing for float16, string_view, and binary_view (#2234)
- **r**: Add R BigQuery driver wrapper (#2235)
- **csharp/src/Drivers/Apache/Spark**: add request_timeout_ms option to allow longer HTTP request length (#2218)
- **go/adbc/driver/snowflake**: add support for a client config file (#2197)
- **csharp/src/Client**: Additional parameter support for DbCommand (#2195)
- **csharp/src/Drivers/Apache/Spark**: add option to ignore TLS/SSL certificate exceptions (#2188)
- **csharp/src/Drivers/Apache/Spark**: Perform scalar data type conversion for Spark over HTTP (#2152)
- **csharp/src/Drivers/Apache/Spark**: Azure HDInsight Spark Documentation (#2164)
- **c/driver/postgresql**: Implement ingestion of list types for PostgreSQL (#2153)
- **csharp/src/Drivers/Apache/Spark**: poc - Support for Apache Spark over HTTP (non-Arrow) (#2018)
- **c/driver/postgresql**: add `arrow.opaque` type metadata (#2122)

### Fix

- **csharp/src/Drivers/Apache**: fix float data type handling for tests on Databricks Spark (#2283)
- **go/adbc/driver/internal/driverbase**: proper unmarshalling for ConstraintColumnNames (#2285)
- **csharp/src/Drivers/Apache**: fix to workaround concurrency issue (#2282)
- **csharp/src/Drivers/Apache**: correctly handle empty response and add Client tests (#2275)
- **csharp/src/Drivers/Apache**: remove interleaved async look-ahead code (#2273)
- **c/driver_manager**: More robust error reporting for errors that occur before AdbcDatabaseInit() (#2266)
- **rust**: implement database/connection constructors without options (#2242)
- **csharp/src/Drivers**: update System.Text.Json to version 8.0.5 because of known vulnerability (#2238)
- **csharp/src/Drivers/Apache/Spark**: correct batch handling for the HiveServer2Reader (#2215)
- **go/adbc/driver/snowflake**: call GetObjects with null catalog at catalog depth (#2194)
- **csharp/src/Drivers/Apache/Spark**: correct BatchSize implementation for base reader (#2199)
- **csharp/src/Drivers/Apache/Spark**: correct precision/scale handling with zeros in fractional portion (#2198)
- **csharp/src/Drivers/BigQuery**: Fixed GBQ driver issue when results.TableReference is null (#2165)
- **go/adbc/driver/snowflake**: fix setting database and schema context after initial connection (#2169)
- **csharp/src/Drivers/Interop/Snowflake**: add test to demonstrate DEFAULT_ROLE behavior (#2151)
- **c/driver/postgresql**: Improve error reporting for queries that error before the COPY header is sent (#2134)

### Refactor

- **c/driver/postgresql**: cleanups for result_helper signatures (#2261)
- **c/driver/postgresql**: Use GetObjectsHelper from framework to build objects (#2189)
- **csharp/src/Drivers/Apache/Spark**: use UTF8 string for data conversion, instead of .NET String (#2192)
- **c/driver/postgresql**: Use Status for error handling in BindStream (#2187)
- **c/driver/postgresql**: Use Status instead of AdbcStatusCode/AdbcError in result helper (#2178)
- **c/driver**: Use non-objects framework components in Postgres driver (#2166)
- **c/driver/postgresql**: Use copy writer in BindStream for parameter binding (#2157)

## ADBC Libraries 16 (2025-01-17)

### Versions

- C/C++/GLib/Go/Python/Ruby: 1.4.0
- C#: 0.16.0
- Java: 0.16.0
- R: 0.16.0
- Rust: 0.16.0

### Breaking Changes

- ⚠️ **rust/driver/snowflake**: return a `Result` from `Builder::from_env` when parsing fails (#2334)

### New Features

- **csharp/src/Client**: parse custom properties from connection string (#2352)
- **csharp/src/Drivers**: introduce Interop.FlightSql driver (#2214)
- **csharp/src/Drivers/Apache**: add connect and query timeout options (#2312)
- **csharp/src/Drivers/Apache**: make Apache driver tests inheritable (#2341)
- **rust/driver/snowflake**: add `adbc_snowflake` crate with Go driver wrapper (#2207)
- ⚠️ **rust/driver/snowflake**: return a `Result` from `Builder::from_env` when parsing fails (#2334)

### Bugfixes

- **c/driver/postgresql**: don't unnecessarily COMMIT (#2412)
- **c/driver/postgresql**: return unknown OIDs as opaque (#2450)
- **ci**: ensure wheels are built with older manylinux (#2351)
- **csharp/src/Apache.Arrow.Adbc/C**: export statement_execute_schema correctly (#2409)
- **csharp/src/Drivers/Apache**: detect sever error when polling for response (#2355)
- **csharp/src/Drivers/BigQuery**: Use job reference instead of job id to get job to avoid interference between different locations (#2433)
- **csharp/src/Drivers/BigQuery**: ensure BigQuery DATE type is Date32 Arrow type (#2446)
- **csharp/src/Drivers/BigQuery**: remove details to have type names match ODBC (#2431)
- **go/adbc/driver/bigquery**: set default project and dataset for new statements (#2342)
- **go/adbc/driver/snowflake**: update default values for fetch params (#2325)
- **java/driver-manager**: typo (#2336)

### Documentation Improvements

- add related work (#2333)
- change Flight SQL driver usage to executable example (#2395)
- remove crosslinking to Arrow Javadocs (#2455)

## ADBC Libraries 17 (2025-03-03)

### Versions

- C/C++/GLib/Go/Python/Ruby: 1.5.0
- C#: 0.17.0
- Java: 0.17.0
- R: 0.17.0
- Rust: 0.17.0

### New Features

- **c/driver**: add support for CMake packages of Go based drivers (#2561)
- **ci/linux-packages/apt**: add support for Ubuntu 24.04 packages (#2482)
- **csharp/src/Apache.Arrow.Adbc**: improved performance of ValueAt helper and AdbcDataReader (#2534)
- **csharp/src/Drivers/Apache**: Add support for Impala ADBC Driver with Refactoring and Unit Tests (#2365)
- **csharp/src/Drivers/BigQuery**: add support for net472 (#2527)
- **csharp/src/Drivers/BigQuery**: use a default project ID if one is not specified (#2471)
- **go/adbc/driver/flightsql**: allow passing arbitrary grpc dial options in NewDatabase (#2563)
- **go/adbc/driver/snowflake**: add query tag option (#2484)
- **go/adbc/driver/snowflake**: implement WithTransporter driver option (#2558)

### Bugfixes

- **c/driver/sqlite**: don't rely on double-quoted strings feature (#2555)
- **go/adbc/driver/flightsql**: Parsing column metadata in FlightSQL driver (#2481)
- **go/adbc/driver/snowflake**: fix GetObjects for VECTOR cols (#2564)
- **go/adbc/driver/snowflake**: use one session for connection (#2494)

### Documentation Improvements

- add SQLite cookbook example for batch size/inference (#2523)
- add stdout/stderr and index support to recipe directive (#2495)
- crosslink to Arrow Javadocs again (#2483)
- fix references to root CONTRIBUTING.md file (#2521)
- update java quickstart to use the PARAM_URI instead of the legacy PARAM_URL (#2530)

## ADBC Libraries 18 (2025-05-02)

### Versions

- C/C++/GLib/Go/Python/Ruby: 1.6.0
- C#: 0.18.0
- Java: 0.18.0
- R: 0.18.0
- Rust: 0.18.0

### New Features

- **c**: Declare dependencies for drivers in Meson configuration (#2746)
- **c/driver/postgresql**: avoid commit/rollback when idle (#2685)
- **csharp**: Add retry-after behavior for 503 responses in Spark ADBC driver (#2664)
- **csharp**: Add support for Prepare to ImportedStatement and to ADO.NET wrapper (#2628)
- **csharp**: Implement CloudFetch for Databricks Spark driver (#2634)
- **csharp**: fix powerbi hang when reading cloudfetch result in Databricks driver (#2747)
- **csharp**: improve handling of StructArrays (#2587)
- **csharp/src/Drivers**: Add Databricks driver (#2672)
- **csharp/src/Drivers/Apache**: Add prefetch functionality to CloudFetch in Spark ADBC driver (#2678)
- **csharp/src/Drivers/Apache**: Add support for Hive ADBC Driver with unit tests (#2540)
- **csharp/src/Drivers/Apache**: Add support for native metadata queries using statement options (#2665)
- **csharp/src/Drivers/Apache**: Custom ssl server certificate validation for Spark, Impala & Hive (#2610)
- **csharp/src/Drivers/Apache**: Performance improvement - Replace TSocketTransport with TBufferedTransport (#2742)
- **csharp/src/Drivers/Apache**: Regenerate Thrift classes based on a newer TCLIService.thrift (#2611)
- **csharp/src/Drivers/Apache**: enhance GetColumns with BASE_TYPE_NAME column (#2695)
- **csharp/src/Drivers/Apache/Spark**: Add Lz4 compression support to arrow batch reader (#2669)
- **csharp/src/Drivers/Apache/Spark**: Add OAuth access token auth type to Csharp Spark Driver (#2579)
- **csharp/src/Drivers/Apache/Spark**: add user agent entry + thrift version for spark http connections (#2711)
- **csharp/src/Drivers/BigQuery**: Add support for AAD/Entra authentication (#2655)
- **csharp/src/Drivers/BigQuery**: add additional billing and timeout properties and test settings (#2566)
- **csharp/src/Drivers/BigQuery**: choose the first project ID if not specified (#2541)
- **csharp/src/Drivers/BigQuery**: support evaluation kind and statement type setting (#2698)
- **csharp/src/Drivers/Databricks**: Add option to enable using direct results for statements (#2737)
- **csharp/src/Drivers/Databricks**: Implement ClientCredentialsProvider (#2743)
- **csharp/src/Drivers/Databricks**: Make Cloud Fetch options configurable at the connection level (#2691)
- **csharp/src/Drivers/Databricks**: Support server side property passthrough (#2692)
- **go/adbc/driver/bigquery**: Return data about table/view partitioning (#2697)
- **go/adbc/driver/flightsql**: Add OAuth Support to Flight Client (#2651)
- **go/adbc/sqldriver**: read from union types (#2637)
- **java/driver/jni**: add JNI bindings to native driver manager (#2401)
- **python/adbc_driver_manager**: add cursor() arg to set options (#2589)
- **python/adbc_driver_manager**: enable DB-API without PyArrow (#2609)

### Bugfixes

- **c**: Add libdl as dependency of driver manager in Meson (#2735)
- **c/driver/postgresql**: avoid crash if closing invalidated result (#2653)
- **c/driver/postgresql**: handle connection options before Init (#2701)
- **ci**: Skip flaky ASAN failures in Meson (#2604)
- **ci**: add missing trigger paths for Linux packages (#2761)
- **ci**: fix MacOS builds for C# (#2606)
- **csharp/src**: Add missing override to ImportedAdbcConnection (#2577)
- **csharp/src/Drivers/Apache**: Fix setting foreign schema/table in GetCrossReference (#2765)
- **csharp/src/Drivers/Apache**: Improve handling of authentication and server type enumeration parsing (#2574)
- **csharp/src/Drivers/Apache**: Set tls enabled to true all HTTP-based drivers, by default (#2667)
- **csharp/src/Drivers/Apache/Thrift**: Generated Thrift-based code should not be exposed publicly (#2710)
- **csharp/src/Drivers/Databricks**: Fix Lz4 compression logic for DatabricksReader (#2690)
- **dev/release**: remove incorrect `-f` from `mamba create` (#2755)
- **dev/release**: use packages.apache.org instead of apache.jfrog.io (#2756)
- **glib**: use -fPIE explicitly for g-ir-scanner (#2758)
- **go**: Use arrow-go in templates instead of arrow/go (#2712)
- **go/adbc/driver/bigquery**: Avoid creating arrow iterator when schema is empty (#2614)
- **go/adbc/driver/bigquery**: Use number of rows (rather than schema) to check if we need an empty arrow iterator (#2674)
- **go/adbc/driver/snowflake**: implement ability to set database options after initialization (#2728)
- **go/adbc/driver/snowflake**: try to suppress stray logs (#2608)
- **python/adbc_driver_postgresql**: handle kwargs in dbapi connect (#2700)
- **rust/core**: remove the Mutex around the FFI driver object (#2736)

### Documentation Improvements

- rework "What exactly is ADBC?" in FAQ (#2763)
- update implementation status table (#2580)
- **rust**: show driver_manager features on docs.rs (#2699)
