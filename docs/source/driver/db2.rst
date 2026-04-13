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

=========
DB2 Driver
=========

The DB2 driver provides access to IBM DB2 databases using the DB2 CLI
(Call Level Interface) / ODBC C API.  Data is fetched from DB2 in row-
oriented form via the CLI and converted into Apache Arrow columnar
format for efficient downstream processing.

Installation
============

The driver requires the IBM DB2 CLI driver (``clidriver``) at both
build time and runtime.  The easiest way to obtain it is through the
``ibm_db`` Python package:

.. code-block:: shell

   pip install ibm_db

The ``clidriver`` is typically next to the ``ibm_db`` install (either
under ``site-packages/clidriver`` or as a sibling of an ``ibm_db``
package directory).  Set ``IBM_DB_HOME`` to that ``clidriver`` path and
ensure the shared libraries are on your library path:

.. code-block:: shell

   export IBM_DB_HOME=$(python -c "import importlib, pathlib; p = pathlib.Path(importlib.import_module('ibm_db').__file__).resolve().parent; print(p.parent / 'clidriver' if p.name == 'ibm_db' else p / 'clidriver')")
   export LD_LIBRARY_PATH="${IBM_DB_HOME}/lib:${LD_LIBRARY_PATH}"  # Linux
   # macOS: also export DYLD_LIBRARY_PATH with the same value (and the ADBC driver dir if needed).

Usage
=====

To connect to a database, supply the ``uri`` parameter when constructing
the :c:struct:`AdbcDatabase`.  The URI should be a DB2 CLI connection
string.  Alternatively, set individual options.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include "arrow-adbc/adbc.h"

         // Ignoring error handling
         struct AdbcDatabase database;
         AdbcDatabaseNew(&database, nullptr);
         AdbcDatabaseSetOption(&database, "driver", "adbc_driver_db2", nullptr);
         AdbcDatabaseSetOption(&database, "uri",
             "DATABASE=mydb;HOSTNAME=localhost;PORT=50000;"
             "PROTOCOL=TCPIP;UID=user;PWD=pass", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import adbc_driver_db2.dbapi

         uri = (
             "DATABASE=mydb;HOSTNAME=localhost;PORT=50000;"
             "PROTOCOL=TCPIP;UID=user;PWD=pass"
         )
         with adbc_driver_db2.dbapi.connect(uri) as conn:
             with conn.cursor() as cur:
                 cur.execute("SELECT * FROM my_table")
                 print(cur.fetchall())

   .. tab-item:: Python (Low-Level)
      :sync: python-lowlevel

      .. code-block:: python

         import adbc_driver_db2
         import adbc_driver_manager

         uri = (
             "DATABASE=mydb;HOSTNAME=localhost;PORT=50000;"
             "PROTOCOL=TCPIP;UID=user;PWD=pass"
         )
         with adbc_driver_db2.connect(uri) as db:
             with adbc_driver_manager.AdbcConnection(db) as conn:
                 with adbc_driver_manager.AdbcStatement(conn) as stmt:
                     stmt.set_sql_query("SELECT * FROM my_table")
                     stream, _ = stmt.execute_query()
                     # Use pyarrow to consume the stream
                     import pyarrow
                     reader = pyarrow.RecordBatchReader._import_from_c(
                         stream.address
                     )
                     table = reader.read_all()

   .. tab-item:: Java (JDBC)
      :sync: java

      .. code-block:: java

         import java.util.HashMap;
         import java.util.Map;
         import org.apache.arrow.adbc.core.*;
         import org.apache.arrow.adbc.driver.jdbc.JdbcDriver;
         import org.apache.arrow.memory.RootAllocator;

         try (var allocator = new RootAllocator()) {
             Map<String, Object> params = new HashMap<>();
             AdbcDriver.PARAM_URI.set(params,
                 "jdbc:db2://localhost:50000/mydb"
                     + "?user=user&password=pass");

             try (var db = new JdbcDriver(allocator).open(params);
                  var conn = db.connect();
                  var stmt = conn.createStatement()) {
                 stmt.setSqlQuery("SELECT * FROM my_table");
                 var result = stmt.executeQuery();
                 // Process ArrowReader result...
             }
         }

Database Options
~~~~~~~~~~~~~~~~

``uri``
    A complete DB2 CLI/ODBC connection string.

``adbc.db2.database``
    The database name.

``adbc.db2.hostname``
    The hostname of the DB2 server.

``adbc.db2.port``
    The port number of the DB2 server.

``adbc.db2.uid`` / ``username``
    The username for authentication.

``adbc.db2.pwd`` / ``password``
    The password for authentication.

If ``uri`` is not set, the driver assembles a connection string from the
individual options.

Supported Features
==================

Bulk Ingestion
--------------

Bulk ingestion is supported.  The driver maps Arrow types to DB2 DDL
types as follows:

- **Boolean, Int8, UInt8, Int16, UInt16** → ``SMALLINT``
- **Int32, UInt32** → ``INTEGER``
- **Int64, UInt64** → ``BIGINT``
- **Float** → ``REAL``
- **Double** → ``DOUBLE``
- **String, Large String** → ``VARCHAR(32672)``
- **Binary, Large Binary, Fixed Size Binary** → ``VARCHAR(32672) FOR BIT DATA``
- **Date32** → ``DATE``
- **Time64** → ``TIME``
- **Timestamp** → ``TIMESTAMP``

All four ingest modes are supported: ``CREATE``, ``APPEND``,
``REPLACE``, and ``CREATE_APPEND``.

Parameterized Queries
---------------------

Parameterized queries are supported using ``?`` as the parameter marker,
following DB2 CLI conventions.  ``AdbcStatementGetParameterSchema`` is
supported via ``SQLDescribeParam``.

Transactions
------------

Transactions are supported.  Autocommit is enabled by default.  Set the
``adbc.connection.autocommit`` option to ``"false"`` to manage
transactions manually.

Cancellation
------------

``AdbcStatementCancel`` is supported via the ODBC ``SQLCancel`` function.

Partitioned Result Sets
-----------------------

Partitioned result sets are not supported.

Type Mapping
------------

The driver maps DB2 CLI SQL types to Arrow types:

.. list-table::
   :header-rows: 1

   * - DB2 Type
     - Arrow Type
   * - ``BIT``
     - ``bool``
   * - ``TINYINT``
     - ``int8``
   * - ``SMALLINT``
     - ``int16``
   * - ``INTEGER``
     - ``int32``
   * - ``BIGINT``
     - ``int64``
   * - ``REAL``
     - ``float``
   * - ``DOUBLE`` / ``FLOAT``
     - ``double``
   * - ``DECIMAL`` / ``NUMERIC``
     - ``string`` (preserves full precision)
   * - ``CHAR`` / ``VARCHAR`` / ``LONGVARCHAR``
     - ``string``
   * - ``WCHAR`` / ``WVARCHAR``
     - ``string`` (UTF-8)
   * - ``BINARY`` / ``VARBINARY``
     - ``binary``
   * - ``DATE``
     - ``date32``
   * - ``TIME``
     - ``time64[us]``
   * - ``TIMESTAMP``
     - ``timestamp[us]``
   * - ``CLOB`` / ``DBCLOB`` / ``XML``
     - ``large_string``
   * - ``BLOB``
     - ``large_binary``
   * - ``GUID``
     - ``fixed_size_binary(36)``

Driver-Specific Options
~~~~~~~~~~~~~~~~~~~~~~~

``adbc.db2.query.batch_rows``
    The number of rows to fetch per batch.  Default: 65536.

Known Limitations
=================

- **DECIMAL/NUMERIC** values are returned as strings rather than Arrow
  ``decimal128`` to preserve full precision without loss.  Applications
  that need numeric types should convert after reading.
- **Binary** values are stored as ``VARCHAR(32672) FOR BIT DATA``.
  DB2 may pad empty binary values, so zero-length binary values do not
  round-trip exactly.  ``BLOB`` columns cannot be used because DB2
  does not allow ``ORDER BY`` on ``BLOB`` columns.
- **Date32** ingest has a known off-by-one issue in the DB2 CLI date
  binding/fetching path.  The pure-integer conversion algorithm is
  correct, but DB2 CLI returns dates shifted by -1 day.
- **Timestamp** ingest does not support negative epoch values (dates
  before 1970-01-01).
- **Timezone-aware timestamps** (``TIMESTAMP WITH TIME ZONE``) are not
  supported.
- **List/Array types** are not supported by DB2 and cannot be ingested.
- **Dictionary-encoded strings** are not supported for ingestion.
- **Temporary tables** cannot be created via the bulk ingest API.
- **Partitioned result sets** are not available.
- The **Transactions** validation test is skipped because the two-
  connection isolation test triggers DB2 lock contention.  Single-
  connection transaction operations (commit, rollback, autocommit
  toggle) are fully functional.

Testing
=======

To run the integration tests locally, start a DB2 instance:

.. code-block:: shell

   docker compose up --wait --detach db2-test

.. tab-set::

   .. tab-item:: C++ Tests
      :sync: cpp

      .. code-block:: shell

         export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst1;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
         cd build && ctest -R driver-db2 -V

   .. tab-item:: Python Tests
      :sync: python

      .. code-block:: shell

         export ADBC_DB2_TEST_URI="DATABASE=testdb;UID=db2inst1;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP"
         cd python/adbc_driver_db2 && pytest -vvx

   .. tab-item:: Java Tests (JDBC)
      :sync: java

      .. code-block:: shell

         export ADBC_JDBC_DB2_URL="localhost:50000/testdb"
         export ADBC_JDBC_DB2_USER="db2inst1"
         export ADBC_JDBC_DB2_PASSWORD="password"
         export ADBC_JDBC_DB2_DATABASE="testdb"
         cd java && mvn test -pl driver/jdbc-validation-db2
