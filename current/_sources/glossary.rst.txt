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

   client API
     The API that an application uses to interact with a database.  May
     abstract over the underlying :term:`wire protocol` and other details.
     For example, ADBC, JDBC, ODBC.

   driver
     A library that implements a :term:`client API` using a :term:`wire
     protocol`.  For example, the ADBC PostgreSQL driver exposes the ADBC
     client API and interacts with a PostgreSQL database via the PostgreSQL
     wire protocol.  The JDBC PostgreSQL driver uses the same wire protocol,
     but exposes the JDBC client API instead.

   driver manager
     A library that helps manage multiple drivers for a given client API.
     For example, the JDBC driver manager can find a appropriate driver
     implementation for a database URI.

     The ADBC driver manager in each language is similar.  In C/C++, it can
     dynamically load drivers so that applications do not have to directly
     link to them.  (Since all drivers expose the same API, their symbols
     would collide otherwise.)  In Python, it loads drivers and provides
     Python bindings on top.

   wire protocol
     The protocol that a database driver uses to interact with a database.
     For example, :external:doc:`format/FlightSql`, the `PostgreSQL wire
     protocol`_, or `Tabular Data Stream`_ (Microsoft SQL Server).  Generally
     not directly used by an application.

.. _Arrow Flight SQL: https://arrow.apache.org/docs/format/FlightSql.html
.. _PostgreSQL wire protocol: https://www.postgresql.org/docs/current/protocol.html
.. _Tabular Data Stream: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/b46a581a-39de-4745-b076-ec4dbb7d13ec
