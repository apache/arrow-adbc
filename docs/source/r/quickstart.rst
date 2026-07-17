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

Here we'll briefly tour basic features of ADBC with R using the PostgreSQL driver.

Installation
============

.. code-block:: r

   install.packages(c("adbcdrivermanager", "arrow", "tibble"))

Basic Example
=============

This example demonstrates connecting to PostgreSQL, executing a query, and reading results.

.. code-block:: r

   library(adbcdrivermanager)

   # Create a driver instance
   drv <- adbc_driver("postgresql")

   # Initialize the database
   db <- adbc_database_init(
     drv,
     uri = "postgresql://user:password@localhost:5432/database"
   )

   # Create a connection
   con <- adbc_connection_init(db)

   # Execute a query and get results
   result <- con |>
     read_adbc("SELECT * FROM my_table") |>
     tibble::as_tibble()

   print(result)

Working with Arrow Data
=======================

ADBC natively returns Arrow data, which you can work with directly:

.. code-block:: r

   # Get results as an Arrow table
   arrow_table <- con |>
     read_adbc("SELECT * FROM my_table") |>
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

   # Using the DBI-style interface
   stmt <- adbc_statement_init(con)
   adbc_statement_set_sql_query(stmt, "SELECT * FROM my_table WHERE id = ?")
   adbc_statement_bind(stmt, list(42))
   result <- adbc_statement_execute_query(stmt) |>
     tibble::as_tibble()

Installing Drivers
==================

You'll need to install an ADBC driver for the database you want to connect to. The easiest way is using `dbc <https://docs.columnar.tech/dbc>`_:

.. code-block:: shell

   # Install dbc
   curl -fsSL https://dbc.sh | sh

   # Install a driver (e.g., PostgreSQL)
   dbc install postgresql

You can also build drivers from source or use other installation methods. See the :doc:`driver documentation </driver/index>` for more details.

Next Steps
==========

- Check out the :doc:`R API documentation <index>` for more details
- See the :doc:`drivers </driver/index>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/r>`_
- Explore the `R source code <https://github.com/apache/arrow-adbc/tree/main/r>`_ for additional examples
