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
Instead of writing code for each individual database, applications can build against the ADBC APIs, and link against drivers that implement the standard.
Additionally, a JDBC/ODBC-style driver manager is provided. This also implements the ADBC APIs, but dynamically loads drivers and dispatches calls to them.

Like JDBC/ODBC, the goal is to provide a generic API for multiple databases, but ADBC is focused on Arrow-based data access for analytics use cases (bulk data retrieval/ingestion), and not the full spectrum of use cases that JDBC/ODBC drivers handle.
Hence, ADBC is complementary to those existing standards.

Like [Arrow Flight SQL][flight-sql], ADBC is an Arrow-based database access API.
But Flight SQL also specifies the wire format and network transport (Flight RPC), while ADBC lets drivers make their own decisions.
Together, ADBC and Flight SQL offer a fully Arrow-native solution for clients and database vendors.
For context, see the [mailing list discussion][ml-discussion] and the original [proposal][proposal].

[flight-sql]: https://arrow.apache.org/docs/format/FlightSql.html
[ml-discussion]: https://lists.apache.org/thread/gnz1kz2rj3rb8rh8qz7l0mv8lvzq254w
[proposal]: https://docs.google.com/document/d/1t7NrC76SyxL_OffATmjzZs2xcj1owdUsIF2WKL_Zw1U/edit#heading=h.r6o6j2navi4c

## Installation

A release has not yet been made.
(The standard is version 1.0.0, but packages themselves are still under development.)
Packages can currently be built and installed from source.

## Documentation

Documentation is forthcoming.
The core API definitions can be read in `adbc.h`.

## Building

For detailed instructions, see CONTRIBUTING.md.
