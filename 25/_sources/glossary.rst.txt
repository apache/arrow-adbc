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

========
Glossary
========

.. glossary::

   Arrow Flight SQL
     A :term:`wire protocol` for data systems that uses Apache Arrow.
     See the :external:doc:`Flight SQL specification <format/FlightSql>`.

   client API
     The API that an application uses to interact with a database.  May
     abstract over the underlying :term:`wire protocol` and other details.  A
     client API is generally standardized so that many applications and
     databases can interoperate through it: examples include ADBC (defined by
     the :doc:`ADBC specification <format/specification>`), JDBC, and ODBC.

   client library
     A library an application uses to access databases from a particular
     language. In ADBC, a client library exposes the ADBC :term:`client API`
     and includes a :term:`driver manager` for loading and using
     :term:`drivers <driver>`. Its API is designed to feel idiomatic in the host
     language (for example, DBAPI in Python, ``database/sql`` in Go, or DBI in
     R). The terms *client library* and *driver manager* are often used
     interchangeably, though strictly the driver manager is the driver-loading
     component within the client library.

   connection
     In the ADBC API, the connection object/struct represents a single
     connection to a database.  Multiple connections may be created from one
     :term:`database`.

   connection profile
     In ADBC, a named, reusable configuration that pairs a :term:`driver` with
     a set of options, stored in a TOML file. A :term:`client library` can load
     a profile by name at connection time, so credentials and settings do not
     have to be specified in application code.

   database
     In the ADBC API, the database object/struct holds state that is shared
     across :term:`connections <connection>`.

     In general usage, a database is the system that an ADBC :term:`driver`
     connects to. This documentation uses the term broadly to refer not only to
     databases proper but also to query engines, data warehouses, data
     lakehouses, cloud data platforms, and the like.

   driver
     Loosely speaking, a library that implements a :term:`client API` using a
     :term:`wire protocol`.  For example, the ADBC PostgreSQL driver exposes
     the ADBC client API and interacts with a PostgreSQL database via the
     PostgreSQL wire protocol.  The JDBC PostgreSQL driver uses the same wire
     protocol, but exposes the JDBC client API instead.

     ADBC drivers can be implemented as libraries in different languages
     including C++, C#, Go, and Rust. A driver can be imported directly into an
     application that's implemented in the same language, or it can be compiled
     into a shared library (a ``.so`` file for Linux, a ``.dylib`` file for
     macOS, or a ``.dll`` file for Windows) and dynamically loaded into an
     application in any supported language using a :term:`driver manager`.

   driver manager
     A component that dynamically loads :term:`drivers <driver>` and forwards an
     application's calls to them. In ADBC, the driver manager is the part of a
     :term:`client library` that loads ADBC drivers. It can load any driver
     built as a shared library, making it possible to use a driver written in
     one language from an application written in another, and simplifies using
     multiple drivers in a single application. The terms *driver manager* and
     *client library* are often used interchangeably.

   driver manifest
     In ADBC, a file (in TOML format) describing a :term:`driver`, with a
     structure defined by the ADBC specification. A :term:`driver manager` can
     load a driver from its manifest, which simplifies the process for users.

   entrypoint
     In ADBC, the name of a function exported by a driver that the
     :term:`driver manager` calls when a driver is loaded to perform any
     initialization required by the driver. The name follows a convention which
     is outlined in :c:type:`AdbcDriverInitFunc` but another name may be used.

   statement
     In the ADBC API, the statement object/struct holds state for executing a
     single query.  The query itself, bind parameters, result sets, and so on are all
     tied to the statement.  Multiple statements may be created from one
     :term:`connection` (though not all drivers will support this).

   Substrait
     `Substrait`_ describes itself as a "cross-language serialization for
     relational algebra".  It operates in a similar space as SQL, but is
     lower-level.  You're unlikely to write a Substrait query by hand, but may
     use tools that opt to generate Substrait instead of SQL for more
     predictable semantics.

   wire protocol
     The actual way a driver connects to a :term:`database`, issues commands,
     gets results, and so forth.  Also called a database client-server protocol.
     For example, :term:`Arrow Flight SQL`, the `PostgreSQL wire protocol`_, or
     `Tabular Data Stream`_ (Microsoft SQL Server).  Generally, an application
     does not implement or use this directly, but instead uses something
     higher-level, like an ADBC, JDBC, or ODBC driver.

.. _PostgreSQL wire protocol: https://www.postgresql.org/docs/current/protocol.html
.. _Substrait: https://substrait.io/
.. _Tabular Data Stream: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/b46a581a-39de-4745-b076-ec4dbb7d13ec
