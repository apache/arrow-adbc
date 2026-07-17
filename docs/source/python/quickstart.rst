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

Here we'll briefly tour basic features of ADBC with Python using the SQLite driver.

Installation
============

.. code-block:: shell

   pip install adbc_driver_manager pyarrow

Installing Drivers
------------------

See :ref:`driver-table-install` for instructions on installing ADBC drivers for
the database you want to connect to. For the example below, you could install
`dbc <https://docs.columnar.tech/dbc>`__ from PyPI with:

.. code-block:: shell

   pipx install dbc

and then install the SQLite driver with:

.. code-block:: shell

   dbc install sqlite

DBAPI (PEP 249)-style API
=========================

If either PyArrow or Polars are installed, ADBC provides a high-level API in the
style of the DBAPI standard.

.. testcleanup:: dbapi

   cursor.close()
   conn.close()

Creating a Connection
---------------------

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> from adbc_driver_manager import dbapi
   >>> conn = dbapi.connect("sqlite")

In application code, the connection must be closed after usage or
memory may leak.  Connections can be used as context managers to
accomplish this.

Creating a Cursor
-----------------

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> cursor = conn.cursor()

In application code, the cursor must be closed after usage or memory
may leak.  Cursors can be used as context managers to accomplish this.

Executing a Query
-----------------

We can execute a query and get the results via the normal,
row-oriented DBAPI interface:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> cursor.execute("SELECT 1, 2.0, 'Hello, world!'")
   <adbc_driver_manager.dbapi.Cursor ...>
   >>> cursor.fetchone()
   (1, 2.0, 'Hello, world!')
   >>> cursor.fetchone()

We can also get the results as Arrow data via a non-standard method:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> cursor.execute("SELECT 1, 2.0, 'Hello, world!'")
   <adbc_driver_manager.dbapi.Cursor ...>
   >>> cursor.fetch_arrow_table()
   pyarrow.Table
   1: int64
   2.0: double
   'Hello, world!': string
   ----
   1: [[1]]
   2.0: [[2]]
   'Hello, world!': [["Hello, world!"]]

Parameterized Queries
---------------------

We can bind parameters in our queries:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> cursor.execute("SELECT ? + 1 AS the_answer", parameters=(41,))
   <adbc_driver_manager.dbapi.Cursor ...>
   >>> cursor.fetch_arrow_table()
   pyarrow.Table
   the_answer: int64
   ----
   the_answer: [[42]]

Ingesting Bulk Data
-------------------

So far we've mostly demonstrated the usual DBAPI interface.  The ADBC
APIs also offer additional methods.  For example, we can insert a
table of Arrow data into a new database table:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> import pyarrow
   >>> table = pyarrow.table([[1, 2], ["a", None]], names=["ints", "strs"])
   >>> cursor.adbc_ingest("sample", table)
   2
   >>> cursor.execute("SELECT COUNT(DISTINCT ints) FROM sample")
   <adbc_driver_manager.dbapi.Cursor ...>
   >>> cursor.fetchall()
   [(2,)]

We can also append to an existing table:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> table = pyarrow.table([[2, 3], [None, "c"]], names=["ints", "strs"])
   >>> cursor.adbc_ingest("sample", table, mode="append")
   2
   >>> cursor.execute("SELECT COUNT(DISTINCT ints) FROM sample")
   <adbc_driver_manager.dbapi.Cursor ...>
   >>> cursor.fetchall()
   [(3,)]

Getting Database/Driver Metadata
--------------------------------

We can get information about the driver and the database using another
extension method, this time on the connection itself:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> conn.adbc_get_info()["vendor_name"]
   'SQLite'
   >>> conn.adbc_get_info()["driver_name"]
   'ADBC SQLite Driver'

We can also query for tables and columns in the database.  This gives
a nested structure describing all the catalogs, schemas, tables, and
columns:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> info = conn.adbc_get_objects().read_all().to_pylist()
   >>> main_catalog = info[0]
   >>> schema = main_catalog["catalog_db_schemas"][0]
   >>> tables = schema["db_schema_tables"]
   >>> tables[0]["table_name"]
   'sample'
   >>> [column["column_name"] for column in tables[0]["table_columns"]]
   ['ints', 'strs']

We can get the Arrow schema of a table:

.. doctest:: dbapi
   :skipif: adbc_driver_manager is None

   >>> conn.adbc_get_table_schema("sample")
   ints: int64
   strs: string

Next Steps
==========

- Check out the :doc:`Python API documentation <index>` for more details
- See the :doc:`drivers </driver/index>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/python>`_
- Explore the `Python source code <https://github.com/apache/arrow-adbc/tree/main/python>`_ for additional examples
