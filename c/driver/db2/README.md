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

# ADBC IBM DB2 Driver

This driver provides an interface to
[IBM DB2](https://www.ibm.com/products/db2) using ADBC.

It uses the DB2 CLI (Call Level Interface) / ODBC API to communicate
with DB2 and converts results into Apache Arrow columnar format using
NanoArrow.

## Building

Dependencies: IBM DB2 CLI headers and library, or a compatible ODBC
driver manager (unixODBC / iODBC).

Set the `DB2_HOME` or `IBM_DB_HOME` environment variable to the DB2
client installation directory, or ensure the ODBC development headers
are available on the system.

```bash
cmake -S c -B build -DADBC_DRIVER_DB2=ON
cmake --build build
```

See [CONTRIBUTING.md](../../../CONTRIBUTING.md) for details.

## Testing

Requires a running DB2 instance. Set `ADBC_DB2_TEST_URI` to a DB2
connection string to run the integration tests.
