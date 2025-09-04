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
     See the :external:doc:`specification <format/FlightSql>`.

   client API
     The API that an application uses to interact with a database.  May
     abstract over the underlying :term:`wire protocol` and other details.
     For example, ADBC, JDBC, ODBC.

   connection
     In ADBC, the connection object/struct represents a single connection to a
     database.  Multiple connections may be created from one :term:`database`.

   database
     In ADBC, the database object/struct holds state that is shared across
     connections.

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
     A library for loading and using :term:`drivers <driver>`. A driver manager
     implements the ADBC API and delegates to dynamically-loaded drivers. It
     simplifies using multiple drivers in a single application and makes it
     possible to use drivers written in any language, regardless of the language
     the application is written in.

     The driver manager in each language is similar.  In C/C++, it can dynamically load drivers
     so that applications do not have to directly link to them.  (Since all
     drivers expose the same API, their symbols would collide otherwise.)  In
     Python, it loads drivers and provides Python bindings on top.

   driver manifest
     A file (in TOML format) describing a :term:`driver`. This file's structure
     is part of the ADBC :doc:`specification <format/specification>`. A
     :term:`driver manager` can load a driver from a which simplifies the
     process for users.

   statement
     In ADBC, the statement object/struct holds state for executing a single
     query.  The query itself, bind parameters, result sets, and so on are all
     tied to the statement.  Multiple statements may be created from one
     :term:`connection` (though not all drivers will support this).

   Substrait
     `Substrait`_ describes itself as a "cross-language serialization for
     relational algebra".  It operates in a similar space as SQL, but is
     lower-level.  You're unlikely to write a Substrait query by hand, but may
     use tools that opt to generate Substrait instead of SQL for more
     predictable semantics.

   wire protocol
     The actual way a database driver connects to a database, issues commands,
     gets results, and so forth.  For example, :term:`Arrow Flight SQL`, the
     `PostgreSQL wire protocol`_, or `Tabular Data Stream`_ (Microsoft SQL
     Server).  Generally, an application is not going to implement and use this
     directly, but instead use something higher-level, like an ADBC, JDBC, or
     ODBC driver.

.. _PostgreSQL wire protocol: https://www.postgresql.org/docs/current/protocol.html
.. _Substrait: https://substrait.io/
.. _Tabular Data Stream: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/b46a581a-39de-4745-b076-ec4dbb7d13ec
