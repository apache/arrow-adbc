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

ADBC in R is implemented as a suite of R packages. Most users will
interact with ADBC via the `adbcdrivermanager <adbcdrivermanager/index.html>`_
package and use drivers that are also distributed as R packages. In
addition to the low-level interface provided by adbcdrivermanager,
you can use ``read_adbc()``, ``write_adbc()`` and ``execute_adbc()``
to quickly interact with an ADBC connection or database.

.. code-block:: r

   library(adbcdrivermanager)

   # Use the driver manager to connect to a database
   db <- adbc_database_init(adbcsqlite::adbcsqlite(), uri = ":memory:")
   con <- adbc_connection_init(db)

   # Write a table
   mtcars |>
     write_adbc(con, "mtcars")

   # Query it
   con |>
     read_adbc("SELECT * from mtcars") |>
     tibble::as_tibble()

   # Clean up
   con |>
     execute_adbc("DROP TABLE mtcars")
   adbc_connection_release(con)
   adbc_database_release(db)

See individual package documentation for installation and usage
details specific to each driver.

---------------------
Package documentation
---------------------

- `adbcdrivermanager <adbcdrivermanager/index.html>`_
- `adbcbigquery <adbcbigquery/index.html>`_
- `adbcflightsql <adbcflightsql/index.html>`_
- `adbcpostgresql <adbcpostgresql/index.html>`_
- `adbcsnowflake <adbcsnowflake/index.html>`_
- `adbcsqlite <adbcsqlite/index.html>`_
