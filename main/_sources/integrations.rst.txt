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

====================
Tools & Integrations
====================

ADBC works alongside many popular data tools and frameworks. This page describes how ADBC fits into each ecosystem.

----

Apache DataFusion
=================

`Apache DataFusion <https://datafusion.apache.org>`_ integrates with ADBC in two ways:

1. **DataFusion is available as an ADBC driver** — the `DataFusion ADBC driver <https://adbc-drivers.org/drivers/datafusion/>`_ lets you run DataFusion queries from any ADBC client library.
2. **ADBC table provider** — the `ADBC table provider for DataFusion <https://github.com/datafusion-contrib/datafusion-table-providers#adbc>`_ lets you query *other* databases from DataFusion. A table provider exposes an external data source as a table in DataFusion, so you can register a database table via an ADBC driver and query it with DataFusion SQL alongside your other data sources.

----

databow
=======

`databow <https://docs.columnar.tech/databow/>`_ is a command-line tool for querying databases via ADBC. It provides an interactive SQL shell with syntax highlighting, formatted output, and support for exporting results to JSON, CSV, or Arrow IPC files.

See the `databow documentation <https://docs.columnar.tech/databow/>`_ for more details.

----

.. _dbc:

dbc
===

`dbc <https://docs.columnar.tech/dbc>`__ is a command-line tool from `Columnar <https://columnar.tech>`__ for installing and managing ADBC drivers on your system. It handles downloading driver binaries, placing them in standard locations, and keeping them up to date—so you don't have to build drivers from source or manage shared libraries by hand.

See the `dbc documentation <https://docs.columnar.tech/dbc>`_ for how to install and use dbc.

----

DuckDB
======

`DuckDB <https://duckdb.org>`_ integrates with ADBC in two ways:

1. **DuckDB itself is an ADBC driver** — DuckDB (technically libduckdb) exposes an ADBC interface so you can connect to a DuckDB database from any ADBC client library. See :doc:`driver/duckdb` for details on using DuckDB as an ADBC driver.
2. **DuckDB extensions** — Two DuckDB community extensions, `adbc <https://duckdb.org/community_extensions/extensions/adbc.html>`_ and `adbc_scanner <https://duckdb.org/community_extensions/extensions/adbc_scanner>`_, let you query *other* databases from DuckDB using ADBC drivers.

----

GDAL
====

`GDAL <https://gdal.org>`_ (Geospatial Data Abstraction Library) supports reading spatial data from databases via ADBC through its `ADBC OGR driver <https://gdal.org/en/stable/drivers/vector/adbc.html>`_ (added in GDAL 3.11). The driver is read-only. This lets you use any ADBC-compatible database as a vector data source in tools like ``ogrinfo`` and ``ogr2ogr``.

See the `GDAL ADBC driver documentation <https://gdal.org/en/stable/drivers/vector/adbc.html>`_ for full details.

----

Go ``database/sql``
===================

The Go ADBC client library provides an adapter so ADBC drivers can be used through Go's standard `database/sql <https://pkg.go.dev/database/sql>`_ interface. This means you can use ADBC drivers with any library or framework that accepts a ``*sql.DB``.

.. code-block:: go

   import (
       "database/sql"
       "github.com/apache/arrow-adbc/go/adbc/sqldriver"
       "github.com/apache/arrow-adbc/go/adbc/drivermgr"
   )

   // Register the ADBC driver manager as a database/sql driver.
   sql.Register("flightsql", sqldriver.Driver{Driver: &drivermgr.Driver{}})

   db, err := sql.Open("flightsql", "uri=grpc://localhost:12345")

See the `Go documentation <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`_ for full details.

----

pandas
======

`pandas <https://pandas.pydata.org>`_ can read query results from any ADBC driver via the ``adbc_driver_manager`` package. Because ADBC collects results as Arrow, the conversion to a pandas ``DataFrame`` is efficient and avoids unnecessary copies.

.. code-block:: python

   import adbc_driver_manager.dbapi as dbapi
   import pandas as pd

   # The postgresql driver must be installed first with: dbc install postgresql
   with dbapi.connect(driver="postgresql", uri="postgresql://localhost:5432/mydb") as conn:
       df = pd.read_sql("SELECT * FROM orders", conn)

ADBC connections are compatible with ``pd.read_sql``, ``pd.read_sql_query``, and ``pd.read_sql_table``.

See the recipe :ref:`Using Pandas and ADBC <recipe-postgresql-pandas>` for more examples.

----

Polars
======

`Polars <https://pola.rs>`_ has native ADBC support via ``read_database`` and ``write_database``. ADBC provides zero-copy Arrow transfer between Polars and the database.

.. code-block:: python

   import adbc_driver_manager.dbapi as dbapi
   import polars as pl

   # The postgresql driver must be installed first with: dbc install postgresql
   with dbapi.connect(driver="postgresql", uri="postgresql://localhost:5432/mydb") as conn:
       # Read from a database
       df = pl.read_database(
           query="SELECT * FROM orders",
           connection=conn,
       )

       # Write to a database
       df.write_database(
           table_name="orders_copy",
           connection=conn,
       )

See the `Polars database documentation <https://docs.pola.rs/user-guide/io/database/>`_ for more details.

----

R adbi
======

In R, the `adbi <https://adbi.r-dbi.org>`__ package provides a `DBI <https://github.com/r-dbi>`__ compliant interface to ADBC drivers.

.. code-block:: r

    library(DBI)

    # Create an SQLite connection via adbi
    con <- dbConnect(adbi::adbi(adbcdrivermanager::adbc_driver("sqlite")))

    # Write a data.frame to a table in SQLite
    dbWriteTable(con, "swiss", datasets::swiss)

    # Query it back
    dbGetQuery(con, "SELECT * from swiss WHERE Agriculture < 40")

    # Take advantage of DBI's Arrow extension API to return results as Arrow (using nanoarrow)
    dbGetQueryArrow(con, "SELECT * from swiss WHERE Agriculture < 40")

R dplyr
=======

In R, `dplyr <https://dplyr.tidyverse.org>`__ accesses databases through the `DBI <https://dbi.r-dbi.org>`__ interface. The ``adbcdrivermanager`` package provides a DBI-compatible backend so you can use any ADBC driver with dplyr and ``dbplyr``.

.. code-block:: r

   library(adbi)
   library(dplyr)

   con <- DBI::dbConnect(
     adbi(adbcdrivermanager::adbc_driver("postgresql")),
     uri = "postgresql://localhost:5432/mydb"
   )

   orders <- tbl(con, "orders") |>
     filter(status == "shipped") |>
     collect()

----

Ruby Active Record
==================

The `activerecord-adbc-adapter <https://github.com/red-data-tools/activerecord-adbc-adapter>`_ gem allows you to use ADBC drivers as Active Record database adapters, enabling efficient Arrow-based database interaction for Rails applications.

See the `activerecord-adbc-adapter documentation <https://www.rubydoc.info/gems/activerecord-adbc-adapter/>`_ for configuration options.

----


Add Your Own Here
=================

Know of an integratoin or tool not listed here? Contributions to this page are welcome via pull requests on `GitHub <https://github.com/apache/arrow-adbc>`_.
