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

The SQLite driver provides access to SQLite databases.

Status
======

The SQLite driver is essentially a "reference" driver that was used
during ADBC development.  It generally supports most ADBC features but
has not received attention to optimization.

Installation
============

The SQLite driver is shipped as a standalone library.

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

Supported Features
==================

Bulk Ingestion
--------------

Bulk ingestion is supported.  The mapping from Arrow types to SQLite
types is the same as below.

Partitioned Result Sets
-----------------------

Partitioned result sets are not supported.

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
