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

# ADBC: Arrow Database Connectivity

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](https://github.com/apache/arrow-adbc/blob/master/LICENSE.txt)

ADBC is an API standard (version 1.0.0) for database access libraries ("drivers") in C, Go, and Java that uses Arrow for result sets and query parameters.
Instead of writing code to extract Arrow data out of each individual database, applications can build against the ADBC APIs, and link against drivers that implement the standard.
Additionally, a JDBC/ODBC-style driver manager is provided. This also implements the ADBC APIs, but dynamically loads drivers and dispatches calls to them.

Like JDBC/ODBC, the goal is to provide a generic API for multiple databases.
ADBC, however, focuses on Arrow-based data access for analytics use cases - primarily bulk data retrieval and ingestion, and not the full spectrum of use cases that JDBC/ODBC support.
Hence, ADBC is complementary to those existing standards.

Like [Arrow Flight SQL][flight-sql], ADBC is an Arrow-based way to work with databases.
But Flight SQL is a database access _protocol_ that also defines the wire format and network transport, so it's only useful if a database specifically implements support for it.
ADBC lets drivers wrap existing database protocols, whether Arrow-native or not.
Together, ADBC and Flight SQL offer a fully Arrow-native solution for clients and database vendors.

For more about ADBC, see the [introductory blog post][arrow-blog].

[arrow-blog]: https://arrow.apache.org/blog/2023/01/05/introducing-arrow-adbc/
[flight-sql]: https://arrow.apache.org/docs/format/FlightSql.html

## Status

ADBC versions the API standard and the implementing libraries separately.

The API standard (version 1.0.0) is considered stable, but enhancements may be made.

Libraries are under development.
For more details, see the [documentation](https://arrow.apache.org/adbc/main/driver/status.html).

## Installation

Please see the [documentation](https://arrow.apache.org/adbc/main/driver/installation.html).

## Documentation

The core API definitions can be read in `adbc.h`.
User documentation can be found at https://arrow.apache.org/adbc

## Development and Contributing

For detailed instructions on how to build the various ADBC libraries, see CONTRIBUTING.md.
