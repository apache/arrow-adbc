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

.. note:: The PostgreSQL driver is in beta.
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

         install.packages("adbcpostgresql")

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
            defer db.Close()

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

Bind Parameters and Prepared Statements
---------------------------------------

The PostgreSQL driver only supports executing prepared statements with
parameters that do not return result sets (basically, an INSERT with
parameters).  Queries that return result sets are difficult with prepared
statements because the driver is built around using COPY for best
performance, which is not supported in this context.

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

Type support is currently limited depending on the type and whether it is
being read or written.

.. list-table:: Arrow type to PostgreSQL type mapping
   :header-rows: 1

   * - Arrow Type
     - As Bind Parameter
     - In Bulk Ingestion [#bulk-ingestion]_

   * - binary
     - BYTEA
     - BYTEA

   * - bool
     - BOOLEAN
     - BOOLEAN

   * - date32
     - DATE
     - DATE

   * - date64
     - ❌
     - ❌

   * - dictionary
     - (as unpacked type)
     - (as unpacked type, only for binary/string)

   * - duration
     - INTERVAL
     - INTERVAL

   * - float32
     - REAL
     - REAL

   * - float64
     - DOUBLE PRECISION
     - DOUBLE PRECISION

   * - int8
     - SMALLINT
     - SMALLINT

   * - int16
     - SMALLINT
     - SMALLINT

   * - int32
     - INTEGER
     - INTEGER

   * - int64
     - BIGINT
     - BIGINT

   * - large_binary
     - ❌
     - ❌

   * - large_string
     - TEXT
     - TEXT

   * - month_day_nano_interval
     - INTERVAL
     - INTERVAL

   * - string
     - TEXT
     - TEXT

   * - timestamp
     - TIMESTAMP [#timestamp]_
     - TIMESTAMP/TIMESTAMP WITH TIMEZONE

.. list-table:: PostgreSQL type to Arrow type mapping
   :header-rows: 1

   * - PostgreSQL Type
     - In Result Set

   * - ARRAY
     - list
   * - BIGINT
     - int64
   * - BINARY
     - binary
   * - BOOLEAN
     - bool
   * - CHAR
     - utf8
   * - DATE
     - date32
   * - DOUBLE PRECISION
     - float64
   * - INTEGER
     - int32
   * - INTERVAL
     - month_day_nano_interval
   * - NUMERIC
     - utf8 [#numeric-utf8]_
   * - REAL
     - float32
   * - SMALLINT
     - int16
   * - TEXT
     - utf8
   * - TIME
     - time64
   * - TIMESTAMP WITH TIME ZONE
     - timestamp[unit, UTC]
   * - TIMESTAMP WITHOUT TIME ZONE
     - timestamp[unit]
   * - VARCHAR
     - utf8

.. [#bulk-ingestion] This is the data type used when creating/appending to a
                     table from Arrow data via the bulk ingestion feature.

.. [#numeric-utf8] NUMERIC types are read as the string representation of the
                   value, because the PostgreSQL NUMERIC type cannot be
                   losslessly converted to the Arrow decimal types.

.. [#timestamp] When binding a timestamp value, the time zone (if present) is
                ignored.  The value will be converted to microseconds and
                adjusted to the PostgreSQL epoch (2000-01-01) and so may
                overflow/underflow; an error will be returned if this would be
                the case.

Software Versions
=================

For Python wheels, the shipped version of the PostgreSQL client libraries is
15.2.  For conda-forge packages, the version of libpq is the same as the
version of libpq in your Conda environment.

The PostgreSQL driver is tested against PostgreSQL versions 11 through 16.
