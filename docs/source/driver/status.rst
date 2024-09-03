.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..   http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

============================
Driver Implementation Status
============================

.. warning:: There is a known problem on macOS x86_64 when using two drivers
             written in Go in the same process (unless working in a pure-Go
             application), where using the second driver may crash.  For more
             details, see `GH-1841
             <https://github.com/apache/arrow-adbc/issues/1841>`_.

Implementation Status
=====================

**Experimental** drivers are not feature-complete and the implementation is still progressing.
**Beta** drivers are (mostly) feature-complete but have only been available for a short time.
**Stable** drivers are (mostly) feature-complete (as much as possible for the underlying database) and have been available/tested for a while.

.. list-table::
   :header-rows: 1

   * - Driver
     - Supported Languages [#supported-languages]_
     - Implementation Language
     - Status

   * - BigQuery
     - C#
     - C#
     - Experimental

   * - Flight SQL (Go)
     - C, Go
     - Go
     - Stable

   * - Flight SQL (Java)
     - Java
     - Java
     - Experimental

   * - JDBC
     - Java
     - Java
     - Experimental

   * - PostgreSQL
     - C
     - C++
     - Stable

   * - SQLite
     - C
     - C
     - Stable

   * - Snowflake
     - C, Go
     - Go
     - Stable

.. [#supported-languages] C drivers are usable from Go, Python, and Ruby as well.

Feature Support
===============

N/A indicates that it is not possible to support this feature in the underlying database.

See individual driver documentation pages for full details.

Bulk Ingestion
    Does the driver support :ref:`bulk ingestion of data <specification-bulk-ingestion>` (creating or appending to a database table from an Arrow table)?

Database Metadata
    Does the driver support functions like :cpp:func:`AdbcConnectionGetObjects` that get metadata about the database catalog, etc.?

Parameterized Queries
    Does the driver support binding query parameters?

Partitioned Data
    Being able to read individual chunks of a (generally distributed)
    result set (:cpp:func:`AdbcStatementExecutePartitions`).

Prepared Statements
    Does the driver support binding query parameters?

Full Type Support
    Does the driver map all database types to/from equivalent Arrow types, as much as is possible?

Select Queries
    Does the driver support queries returning result sets?

SQL
    Does the driver support submitting SQL queries?

Transactions
    Does the driver support explicit transactions (the default is to assume auto-commit)?

Substrait
    Does the driver support submitting Substrait plans?

Update Queries
    Does the driver support queries not returning result sets?

.. list-table:: General features
   :header-rows: 1

   * - Driver
     - Full Type Support
     - SQL
     - Substrait

   * - Flight SQL (Go)
     - Y
     - Y
     - N

   * - Flight SQL (Java)
     - Y
     - Y
     - N

   * - JDBC
     - N
     - Y
     - N/A

   * - PostgreSQL
     - N
     - Y
     - N/A

   * - SQLite
     - Y
     - Y
     - N/A

.. list-table:: Statement/query-level features
   :header-rows: 1

   * - Driver
     - Incremental Queries
     - Partitioned Data
     - Parameterized Queries
     - Prepared Statements
     - Select Queries
     - Update Queries

   * - Flight SQL (Go)
     - Y
     - Y
     - Y
     - Y
     - Y
     - Y

   * - Flight SQL (Java)
     - N
     - Y
     - Y
     - Y
     - Y
     - Y

   * - JDBC
     - N/A
     - N/A
     - Y
     - Y
     - Y
     - Y

   * - PostgreSQL
     - N/A
     - N/A
     - Y [#postgresql-prepared]_
     - Y
     - Y
     - Y

   * - SQLite
     - N/A
     - N/A
     - Y
     - Y
     - Y
     - Y

.. [#postgresql-prepared] The PostgreSQL driver only supports executing
   prepared statements with parameters that do not return result sets
   (basically, an INSERT with parameters).  Queries that return result sets
   are difficult with prepared statements because the driver is built around
   using COPY for best performance, which is not supported in this context.

.. list-table:: Connection/database-level features
   :header-rows: 1

   * - Driver
     - Bulk Ingestion
     - Database Metadata (catalogs, etc.)
     - Transactions

   * - Flight SQL (Go)
     - N
     - Y
     - Y

   * - Flight SQL (Java)
     - Y
     - Y
     - N

   * - JDBC
     - Y
     - Y
     - N

   * - PostgreSQL
     - Y
     - Y
     - Y

   * - SQLite
     - Y
     - Y
     - Y
