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

=============
SQLite Driver
=============

The SQLite driver provides access to any database that supports the
Postgres wire format.

Installation
============

The SQLite driver is shipped as a standalone library.  See
:ref:`Installation <cpp-install-sqlite>`.

Usage
=====

To connect to a database, supply the "uri" parameter when constructing
the :cpp:class:`AdbcDatabase`.  This should be a filename or `URI
filename <https://www.sqlite.org/c3ref/open.html#urifilenamesinsqlite3open>`_.
If omitted, it will default to an in-memory database, but one that is
shared across all connections.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include "adbc.h"

         // Ignoring error handling
         struct AdbcDatabase database;
         AdbcDatabaseNew(&database, nullptr);
         AdbcDatabaseSetOption(&database, "uri", "file:mydb.db", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_sqlite.dbapi

         with adbc_driver_sqlite.dbapi.connect() as conn:
             pass
