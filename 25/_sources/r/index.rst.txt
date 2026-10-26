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

===
R
===

ADBC is available in R through the `adbcdrivermanager
<adbcdrivermanager/index.html>`_ package, which provides a low-level interface
along with the ``read_adbc()``, ``write_adbc()``, and ``execute_adbc()`` helpers
for quickly interacting with an ADBC connection or database.

Installation
============

.. code-block:: r

   install.packages(c("adbcdrivermanager", "arrow", "tibble"))

Installing Drivers
------------------

See :ref:`driver-table-install` for instructions on installing ADBC drivers for
the database you want to connect to. For the example below, you could install
`dbc <https://docs.columnar.tech/dbc>`__ and then install the SQLite driver with:

.. code-block:: shell

   dbc install sqlite

Basic Example
=============

This example demonstrates connecting to SQLite, writing a table, querying it,
and cleaning up.

.. code-block:: r

   library(adbcdrivermanager)

   # Create a driver instance
   drv <- adbc_driver("sqlite")

   # Initialize the database
   db <- adbc_database_init(drv, uri = ":memory:")

   # Create a connection
   con <- adbc_connection_init(db)

   # Write a table
   mtcars |>
     write_adbc(con, "mtcars")

   # Query it
   con |>
     read_adbc("SELECT * FROM mtcars") |>
     tibble::as_tibble()

   # Clean up
   con |>
     execute_adbc("DROP TABLE mtcars")
   adbc_connection_release(con)
   adbc_database_release(db)

Working with Arrow Data
=======================

ADBC natively returns Arrow data, which you can work with directly:

.. code-block:: r

   # Get results as an Arrow table
   arrow_table <- con |>
     read_adbc("SELECT * FROM mtcars") |>
     arrow::as_arrow_table()

   # For large result sets, use a record batch reader
   reader <- con |>
     read_adbc("SELECT * FROM large_table") |>
     arrow::as_record_batch_reader()

   # Process in chunks
   while (!is.null(batch <- reader$read_next_batch())) {
     # Process batch
     print(nrow(batch))
   }

Parameterized Queries
=====================

You can use parameterized queries for safe SQL execution:

.. code-block:: r

   stmt <- adbc_statement_init(con)
   adbc_statement_set_sql_query(stmt, "DELETE FROM mtcars WHERE cyl = ?")
   adbc_statement_bind(stmt, data.frame(8L))
   adbc_statement_execute_query(stmt)

Package Documentation
=====================

The ``adbcdrivermanager`` package is the R client library, and provides
the driver manager used in the examples above:

- `adbcdrivermanager <adbcdrivermanager/index.html>`_

A few drivers are also built as R packages and distributed on CRAN:

- `adbcflightsql <adbcflightsql/index.html>`_
- `adbcpostgresql <adbcpostgresql/index.html>`_
- `adbcsqlite <adbcsqlite/index.html>`_

In general, though, we recommend installing drivers as shared libraries (see
`Installing Drivers`_), which works with far more databases than are available
as R packages.

Next Steps
==========

- See the :doc:`drivers </driver/index>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/r>`_
- Explore the `R source code <https://github.com/apache/arrow-adbc/tree/main/r>`_ for additional examples
