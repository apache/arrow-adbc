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

==========
Db2 Driver
==========

.. note::

   This driver is in the early stages of development.  The current
   release implements **connection management only**: opening and
   closing :c:struct:`AdbcDatabase` / :c:struct:`AdbcConnection`
   handles, parsing connection options, authenticating against the
   server, and mapping Db2 SQLSTATEs to ADBC error codes.

   Statement execution, metadata retrieval (``GetObjects``,
   ``GetTableSchema``, ...), transaction management, and bulk
   ingestion are **not yet implemented** and will be added in
   subsequent pull requests.  Calling those entry points returns
   ``ADBC_STATUS_NOT_IMPLEMENTED``.

The Db2 driver targets `IBM Db2 LUW
<https://www.ibm.com/products/db2>`_ and is built on the IBM Db2
Call Level Interface (CLI / ODBC).

Installation
============

The driver requires the IBM Data Server Driver Package (the
"clidriver"), which provides ``sqlcli1.h`` and the ``libdb2`` shared
library.  Generic ODBC driver managers (unixODBC, iODBC) are not
used as a fallback because Db2-specific SQLSTATE classes are required
for accurate error mapping.

The simplest way to obtain the clidriver is via the ``ibm_db`` Python
package:

.. code-block:: shell

   pip install ibm_db
   export IBM_DB_HOME=$(python -c "import importlib, pathlib; \
       p = pathlib.Path(importlib.import_module('ibm_db').__file__).resolve().parent; \
       print(p.parent / 'clidriver' if p.name == 'ibm_db' else p / 'clidriver')")
   export LD_LIBRARY_PATH="${IBM_DB_HOME}/lib:${LD_LIBRARY_PATH}"  # Linux
   # macOS users: export DYLD_LIBRARY_PATH with the same value.

Alternatively, point CMake at any clidriver install via
``DB2_HOME``, ``IBM_DB_HOME``, or by setting ``DB2_INCLUDE_DIR`` and
``DB2_LIBRARY`` directly.

Build the driver with:

.. code-block:: shell

   cmake -S c -B build -DADBC_DRIVER_DB2=ON
   cmake --build build

Usage
=====

Configure an :c:struct:`AdbcDatabase` with either a complete Db2 CLI
connection string via the standard ``"uri"`` option, or with the
individual ``adbc.db2.*`` options:

.. code-block:: cpp

   #include <arrow-adbc/adbc.h>
   #include <arrow-adbc/driver/db2.h>

   struct AdbcDatabase database;
   AdbcDatabaseNew(&database, /*error=*/nullptr);
   AdbcDatabaseSetOption(
       &database, "uri",
       "DATABASE=mydb;HOSTNAME=localhost;PORT=50000;"
       "PROTOCOL=TCPIP;UID=user;PWD=pass",
       /*error=*/nullptr);
   AdbcDatabaseInit(&database, /*error=*/nullptr);

   struct AdbcConnection connection;
   AdbcConnectionNew(&connection, /*error=*/nullptr);
   AdbcConnectionInit(&connection, &database, /*error=*/nullptr);

   // ...

   AdbcConnectionRelease(&connection, /*error=*/nullptr);
   AdbcDatabaseRelease(&database, /*error=*/nullptr);

Database Options
~~~~~~~~~~~~~~~~

``uri``
    A complete Db2 CLI connection string.  When set, the per-field
    options below are ignored.

``adbc.db2.database``
    The Db2 database (catalog) name.

``adbc.db2.hostname``
    The Db2 server hostname.

``adbc.db2.port``
    The Db2 server port (as a string, e.g. ``"50000"``).

``adbc.db2.uid`` / ``username``
    The Db2 user identifier.  ``"username"`` is accepted as a synonym.

``adbc.db2.pwd`` / ``password``
    The Db2 password.  ``"password"`` is accepted as a synonym.

Error Mapping
=============

Db2 ``SQLSTATE`` classes are mapped to ADBC status codes:

.. list-table::
   :header-rows: 1

   * - SQLSTATE class
     - ADBC status
   * - ``08`` (connection exception)
     - ``ADBC_STATUS_IO``
   * - ``22`` (data exception)
     - ``ADBC_STATUS_INVALID_ARGUMENT``
   * - ``23`` (constraint violation)
     - ``ADBC_STATUS_ALREADY_EXISTS``
   * - ``28`` (authorization)
     - ``ADBC_STATUS_UNAUTHENTICATED``
   * - ``42`` (syntax / access)
     - ``ADBC_STATUS_INVALID_ARGUMENT`` (``42S02`` / ``42704`` → ``ADBC_STATUS_NOT_FOUND``)
   * - ``HY008``
     - ``ADBC_STATUS_CANCELLED``
   * - ``HY010``
     - ``ADBC_STATUS_INVALID_STATE``
   * - ``IM`` (driver manager)
     - ``ADBC_STATUS_INTERNAL``

The first diagnostic record's SQLSTATE and native error code are also
attached as error details (``db2.sqlstate``, ``db2.native_error``).

Testing
=======

Set ``ADBC_DB2_TEST_URI`` to a reachable Db2 CLI connection string and
run the C++ test suite:

.. code-block:: shell

   export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst1;PWD=password;\
   HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
   ctest --test-dir build -L driver-db2 --output-on-failure

When ``ADBC_DB2_TEST_URI`` is unset the tests skip automatically.
