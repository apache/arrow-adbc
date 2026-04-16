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
Quickstart
==========

Here we'll briefly tour basic features of ADBC with the SQLite driver for
Node.js.

Installation
============

.. code-block:: shell

   npm install @apache-arrow/adbc-driver-manager apache-arrow

.. note::

   The ``@apache-arrow/adbc-driver-manager`` package includes TypeScript
   types so you don't need a separate types package to get types.

.. note::

   Node.js 22+, Deno 2.0+, or Bun 1.1+ are required.

To use any driver with the JavaScript driver manager, you'll need install the
appropriate driver shared library for your platform separately and pass the
absolute path to the driver manager.

Some examples for the SQLite driver are provided below for convenience:

`conda-forge <https://conda-forge.org/>`_:

.. code-block:: shell

   conda create -n adbc-sqlite -c conda-forge adbc-driver-sqlite
   conda run -n adbc-sqlite python -c "import adbc_driver_sqlite; print(adbc_driver_sqlite._driver_path())"

`PyPI <https://pypi.org>`_:

.. code-block:: shell

   python -m venv .venv
   source .venv/bin/activate
   pip install adbc-driver-sqlite
   python -c "import adbc_driver_sqlite; print(adbc_driver_sqlite._driver_path())"

For apt and dnf packages, see :doc:`/driver/installation`.

Creating a Database and Connection
==================================

The entry point is ``AdbcDatabase``.  Pass a ``driver`` option to identify the
backend — either a short name (resolved via :doc:`/format/driver_manifests`
search paths) or absolute path to a driver shared library:

.. code-block:: javascript

   import { AdbcDatabase } from '@apache-arrow/adbc-driver-manager';

   // Open a connection to an in-memory SQLite database.
   const db = new AdbcDatabase({ driver: 'sqlite' });
   const conn = await db.connect();

Executing a Query
=================

To run a query, use ``AdbcConnection.query``, which returns an Apache Arrow
``Table`` containing the full result:

.. code-block:: javascript

   const table = await conn.query("SELECT 1 AS id, 'hello' AS greeting");
   console.log(table.getChild('greeting')?.get(0));  // "hello"

For large result sets, use ``AdbcConnection.queryStream`` instead. It
returns a ``RecordBatchReader`` that yields results one batch at a time without
loading the entire result into memory:

.. code-block:: javascript

   const reader = await conn.queryStream('SELECT * FROM large_table');
   for await (const batch of reader) {
     console.log(`Processing batch of ${batch.numRows} rows`);
   }

Executing Updates
=================

Use ``AdbcConnection.execute`` to execute statements that do not return rows
(INSERT, UPDATE, DELETE, DDL).  It returns the number of rows affected:

.. code-block:: javascript

   await conn.execute('CREATE TABLE users (id INTEGER, name TEXT)');
   const affected = await conn.execute(
     "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob')"
   );
   console.log(affected); // 2

Parameterized Queries
=====================

Parameters are passed as an Apache Arrow ``Table`` whose columns correspond to
the bind parameters (``?`` placeholders) in the SQL statement:

.. code-block:: javascript

   import { tableFromArrays } from 'apache-arrow';

   // Bind a single row of parameters.
   const params = tableFromArrays({ id: [1] });
   const result = await conn.query('SELECT name FROM users WHERE id = ?', params);
   console.log(result.getChild('name')?.get(0));  // "Alice"

The same approach works with ``AdbcConnection.execute`` for DML:

.. code-block:: javascript

   const params = tableFromArrays({ id: [2], name: ['Charlie'] });
   await conn.execute('INSERT INTO users VALUES (?, ?)', params);

Ingesting Bulk Data
===================

``AdbcConnection.ingest`` inserts an Arrow ``Table`` into a database
table in a single call, avoiding per-row overhead:

.. code-block:: javascript

   import { tableFromArrays } from 'apache-arrow';
   import { IngestMode } from '@apache-arrow/adbc-driver-manager';

   const data = tableFromArrays({
     id: [1, 2, 3],
     name: ['alice', 'bob', 'carol'],
   });

   // Create a new table and insert the data.
   await conn.ingest('sample', data);

   // Append more rows to the existing table.
   const more = tableFromArrays({ id: [4], name: ['dave'] });
   await conn.ingest('sample', more, { mode: IngestMode.Append });

For datasets that do not fit in memory use ``AdbcConnection.ingestStream`` with
a ``RecordBatchReader``:

.. code-block:: javascript

   import { RecordBatchReader, tableToIPC, tableFromArrays } from 'apache-arrow';

   const data = tableFromArrays({ id: [1, 2, 3] });
   const reader = RecordBatchReader.from(tableToIPC(data, 'stream'));
   await conn.ingestStream('streaming_table', reader);

Getting Database Metadata
=========================

``AdbcConnection.getObjects`` returns a nested Arrow structure describing all
catalogs, schemas, and tables in the database:

.. code-block:: javascript

   const objects = await conn.getObjects({ depth: 3, tableName: 'sample' });

   const dbSchemas = objects.getChild('catalog_db_schemas')?.get(0);
   const tables = dbSchemas?.get(0)?.db_schema_tables;
   console.log(tables?.get(0)?.table_name);  // "sample"

``AdbcConnection.getTableSchema`` returns the Arrow schema for a specific table:

.. code-block:: javascript

   const schema = await conn.getTableSchema({ tableName: 'sample' });
   console.log(schema.fields.map(f => f.name));  // ["id", "name"]

``AdbcConnection.getTableTypes`` lists the types of table objects supported by
the database:

.. code-block:: javascript

   const types = await conn.getTableTypes();
   const typeNames = Array.from(
     { length: types.numRows },
     (_, i) => types.getChild('table_type')?.get(i)
   );
   console.log(typeNames);  // ["table", "view"]

Transactions
============

By default, connections operate in autocommit mode.  Disable autocommit to
manage transactions manually:

.. code-block:: javascript

   conn.setAutoCommit(false);

   await conn.execute("INSERT INTO users VALUES (99, 'temp')");
   await conn.rollback();  // row is not persisted

   await conn.execute("INSERT INTO users VALUES (100, 'permanent')");
   await conn.commit();   // row is persisted

Low-Level Statement API
=======================

``AdbcStatement`` gives direct access to ADBC's statement lifecycle for use
cases that require more control, such as binding parameters separately from
execution or reusing a statement across multiple queries:

.. code-block:: javascript

   const stmt = await conn.createStatement();

   await stmt.setSqlQuery('SELECT id, name FROM users WHERE id = ?');
   const params = tableFromArrays({ id: [1] });
   await stmt.bind(params);

   const reader = await stmt.executeQuery();
   for await (const batch of reader) {
     console.log(batch.numRows);
   }

   await stmt.close();
