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

=================
PostgreSQL Driver
=================

The PostgreSQL driver provides access to any database that supports
the PostgreSQL wire format.  It wraps `libpq`_, the client library for
PostgreSQL.  The project owes credit to 0x0L's `pgeon`_ for the
overall approach.

.. note:: This project is not affiliated with PostgreSQL in any way.

.. _libpq: https://www.postgresql.org/docs/current/libpq.html
.. _pgeon: https://github.com/0x0L/pgeon

Installation
============

The PostgreSQL driver is shipped as a standalone library.  See
:ref:`Installation <cpp-install-libpq>`.

Usage
=====

To connect to a database, supply the "uri" parameter when constructing
the :cpp:class:`AdbcDatabase`.  This should be a `connection URI
<https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING>`_.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include "adbc.h"

         // Ignoring error handling
         struct AdbcDatabase database;
         AdbcDatabaseNew(&database, nullptr);
         AdbcDatabaseSetOption(&database, "uri", "postgresql://localhost:5433", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_postgresql.dbapi


         uri = "postgresql://localhost:5433"
         with adbc_driver_postgresql.dbapi.connect(uri) as conn:
             pass
