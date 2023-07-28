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

**Available for:** C/C++, GLib/Ruby, Go, Python, R

The PostgreSQL driver provides access to any database that supports
the PostgreSQL wire format.  It wraps `libpq`_, the client library for
PostgreSQL.  The project owes credit to 0x0L's `pgeon`_ for the
overall approach.

.. note:: This project is not affiliated with PostgreSQL in any way.

.. _libpq: https://www.postgresql.org/docs/current/libpq.html
.. _pgeon: https://github.com/0x0L/pgeon

.. note:: The PostgreSQL driver is experimental.
          Performance/optimization and support for complex types and
          different ADBC features is still ongoing.

Installation
============

.. tab-set::

   .. tab-item:: C/C++
      :sync: cpp

      For conda-forge users:

      .. code-block:: shell

         mamba install libadbc-driver-postgresql

   .. tab-item:: Go
      :sync: go

      Install the C/C++ package and use the Go driver manager.
      Requires CGO.

      .. code-block:: shell

         go get github.com/apache/arrow-adbc/go/adbc/drivermgr

   .. tab-item:: Python
      :sync: python

      .. code-block:: shell

         # For conda-forge
         mamba install adbc-driver-postgresql

         # For pip
         pip install adbc_driver_postgresql

   .. tab-item:: R
      :sync: r

      .. code-block:: r

         # install.packages("pak")
         pak::pak("apache/arrow-adbc/r/adbcpostgresql")

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

   .. tab-item:: Go
      :sync: go

      You must have `libadbc_driver_postgresql.so` on your LD_LIBRARY_PATH,
      or in the same directory as the executable when you run this. This
      requires CGO and loads the C++ ADBC postgresql driver.

      .. code-block:: go

         import (
            "context"

            "github.com/apache/arrow-adbc/go/adbc"
            "github.com/apache/arrow-adbc/go/adbc/drivermgr"
         )

         func main() {
            var drv drivermgr.Driver
            db, err := drv.NewDatabase(map[string]string{
               "driver": "adbc_driver_postgresql",
               adbc.OptionKeyURI: "postgresql://user:pass@localhost:5433/postgres",
            })
            if err != nil {
               // handle error
            }

            cnxn, err := db.Open(context.Background())
            if err != nil {
               // handle error
            }
            defer cnxn.Close()
         }

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_postgresql.dbapi

         uri = "postgresql://user:pass@localhost:5433/postgres"
         with adbc_driver_postgresql.dbapi.connect(uri) as conn:
             pass

      For more examples, see :doc:`../python/recipe/postgresql`.

   .. tab-item:: R
      :sync: r

      .. code-block:: r

         library(adbcdrivermanager)

         # Use the driver manager to connect to a database
         uri <- Sys.getenv("ADBC_POSTGRESQL_TEST_URI")
         db <- adbc_database_init(adbcpostgresql::adbcpostgresql(), uri = uri)
         con <- adbc_connection_init(db)

Supported Features
==================

The PostgreSQL driver mostly supports features defined in the ADBC API
specification 1.0.0, but not all cases are fully implemented
(particularly around bind parameters and prepared statements).

Bulk Ingestion
--------------

Bulk ingestion is supported.  The mapping from Arrow types to
PostgreSQL types is the same as below.

Partitioned Result Sets
-----------------------

Partitioned result sets are not supported.

Transactions
------------

Transactions are supported.

Type Support
------------

PostgreSQL allows defining new types at runtime, so the driver must
build a mapping of available types.  This is currently done once at
startup.

Type support is currently limited.  Parameter binding and bulk
ingestion support int16, int32, int64, and string.  Reading result
sets is limited to int32, int64, float, double, and string.
