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

**Available for:** C/C++, GLib/Ruby, Go, Python, R

The SQLite driver provides access to SQLite databases.

This driver is essentially a "reference" driver that was used during
ADBC development.  It generally supports most ADBC features but has
not received attention to optimization.

Installation
============

.. tab-set::

   .. tab-item:: C/C++
      :sync: cpp

      For conda-forge users:

      .. code-block:: shell

         mamba install libadbc-driver-sqlite

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
         mamba install adbc-driver-sqlite

         # For pip
         pip install adbc_driver_sqlite

   .. tab-item:: R
      :sync: r

      .. code-block:: r

         install.packages("adbcsqlite")

Usage
=====

To connect to a database, supply the "uri" parameter when constructing
the :c:struct:`AdbcDatabase`.  This should be a filename or `URI
filename <https://www.sqlite.org/c3ref/open.html#urifilenamesinsqlite3open>`_.
If omitted, it will default to an in-memory database, but one that is
shared across all connections.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include "arrow-adbc/adbc.h"

         // Ignoring error handling
         struct AdbcDatabase database;
         AdbcDatabaseNew(&database, nullptr);
         AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", nullptr);
         AdbcDatabaseSetOption(&database, "uri", "<sqlite uri>", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_sqlite.dbapi

         with adbc_driver_sqlite.dbapi.connect() as conn:
             pass

      For more examples, see :doc:`../python/recipe/sqlite`.

   .. tab-item:: R
      :sync: r

      .. code-block:: r

         library(adbcdrivermanager)

         # Use the driver manager to connect to a database
         db <- adbc_database_init(adbcsqlite::adbcsqlite(), uri = ":memory:")
         con <- adbc_connection_init(db)

   .. tab-item:: Go
      :sync: go

      You must have `libadbc_driver_sqlite.so` on your LD_LIBRARY_PATH,
      or in the same directory as the executable when you run this. This
      requires CGO and loads the C++ ADBC sqlite driver.

      .. code-block:: go

         import (
            "context"

            "github.com/apache/arrow-adbc/go/adbc"
            "github.com/apache/arrow-adbc/go/adbc/drivermgr"
         )

         func main() {
            var drv drivermgr.Driver
            db, err := drv.NewDatabase(map[string]string{
               "driver": "adbc_driver_sqlite",
               adbc.OptionKeyURI: "<sqlite uri>",
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

Supported Features
==================

Bulk Ingestion
--------------

Bulk ingestion is supported.  The mapping from Arrow types to SQLite
types is the same as below.

Partitioned Result Sets
-----------------------

Partitioned result sets are not supported.

Run-Time Loadable Extensions
----------------------------

ADBC allows loading SQLite extensions.  For details on extensions themselves,
see `"Run-Time Loadable Extensions" <https://www.sqlite.org/loadext.html>`_ in
the SQLite documentation.

To load an extension, three things are necessary:

1. Enable extension loading by setting
2. Set the path
3. Set the entrypoint

These options can only be set after the connection is fully initialized with
:c:func:`AdbcConnectionInit`.

Options
~~~~~~~

``adbc.sqlite.load_extension.enabled``
    Whether to enable ("true") or disable ("false") extension loading.  The
    default is disabled.

``adbc.sqlite.load_extension.path``
    To load an extension, first set this option to the path to the extension
    to load.  This will not load the extension yet.

``adbc.sqlite.load_extension.entrypoint``
    After setting the path, set the option to the entrypoint in the extension
    (or NULL) to actually load the extension.

Example
~~~~~~~

.. tab-set::

   .. tab-item:: C/C++
      :sync: cpp

      .. code-block:: cpp

         // TODO

   .. tab-item:: Go
      :sync: go

      .. code-block:: go

         // TODO

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_sqlite.dbapi as dbapi

         with dbapi.connect() as conn:
             conn.enable_load_extension(True)
             conn.load_extension("path/to/extension.so")

      The driver implements the same API as the Python standard library
      ``sqlite3`` module, so packages built for it should also work.  For
      example, `sqlite-zstd <https://github.com/phiresky/sqlite-zstd>`_:

      .. code-block:: python

        import adbc_driver_sqlite.dbapi as dbapi
        import sqlite_zstd

        with dbapi.connect() as conn:
            conn.enable_load_extension(True)
            sqlite_zstd.load(conn)

   .. tab-item:: R
      :sync: r

      .. code-block:: shell

         # TODO

Transactions
------------

Transactions are supported.

Type Inference/Type Support
---------------------------

SQLite does not enforce that values in a column have the same type.
The SQLite driver will attempt to infer the best Arrow type for a
column as the result set is read.  When reading the first batch of
data, the driver will be in "type promotion" mode.  The inferred type
of each column begins as INT64, and will convert to DOUBLE, then
STRING, if needed.  After that, reading more batches will attempt to
convert to the inferred types.  An error will be raised if this is not
possible (e.g. if a string value is read but the column was inferred
to be of type INT64).

In the future, other behaviors may also be supported.

Bound parameters will be translated to SQLite's integer,
floating-point, or text types as appropriate.  Supported Arrow types
are: signed and unsigned integers, (large) strings, float, and double.

Driver-specific options:

``adbc.sqlite.query.batch_rows``
    The size of batches to read.  Hence, this also controls how many
    rows are read to infer the Arrow type.

Software Versions
=================

For Python wheels, the shipped version of SQLite is 3.40.1.  For conda-forge
packages, the version of sqlite is the same as the version of sqlite in your
Conda environment.
