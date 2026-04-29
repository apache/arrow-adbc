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

# ADBC IBM Db2 Driver

An ADBC driver for [IBM Db2 LUW](https://www.ibm.com/products/db2),
implemented on top of the Db2 CLI / ODBC API.

> **Status:** Initial scope is connection management only.  The driver
> can establish, configure, and tear down connections, and surfaces
> Db2 SQLSTATEs as ADBC error codes.  Statement execution, metadata
> APIs, transactions, and bulk ingestion will be added in follow-up
> pull requests.

## Building

Requires the IBM Data Server Driver Package (the so-called
"clidriver"), which provides `sqlcli1.h` and `libdb2`.  Generic ODBC
driver managers (unixODBC, iODBC) are not used as a fallback because
the driver relies on Db2-specific SQLSTATE classes for error mapping.

Point CMake at the clidriver via `DB2_HOME` (set by IBM tooling) or
`IBM_DB_HOME` (set by `pip install ibm_db`):

```bash
export DB2_HOME=/opt/ibm/db2/V11.5/clidriver   # or IBM_DB_HOME=...
cmake -S c -B build -DADBC_DRIVER_DB2=ON
cmake --build build
```

## Testing

The C++ test suite expects a reachable Db2 instance.  Set
`ADBC_DB2_TEST_URI` to a Db2 CLI connection string and run:

```bash
ctest --test-dir build -L driver-db2 --output-on-failure
```

When `ADBC_DB2_TEST_URI` is unset, all DB-bound tests skip.
