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

.. _dbc:

dbc
===

`dbc <https://docs.columnar.tech/dbc>`_ is a command-line tool from `Columnar <https://columnar.tech>`_ for installing and managing ADBC drivers on your system. It handles downloading driver binaries, placing them in standard locations, and keeping them up to date — so you don't have to build drivers from source or manage shared libraries by hand.

Installation
------------

.. tab-set::

   .. tab-item:: macOS / Linux (shell)

      .. code-block:: bash

         curl -LsSf https://dbc.columnar.tech/install.sh | sh

   .. tab-item:: Homebrew

      .. code-block:: bash

         brew install columnar-tech/tap/dbc

   .. tab-item:: uv

      .. code-block:: bash

         uv tool install dbc

   .. tab-item:: pipx

      .. code-block:: bash

         pipx install dbc

   .. tab-item:: Windows (PowerShell)

      .. code-block:: powershell

         powershell -ExecutionPolicy ByPass -c "irm https://dbc.columnar.tech/install.ps1 | iex"

   .. tab-item:: Windows (winget)

      .. code-block:: bash

         winget install dbc

Usage
-----

Once installed, use ``dbc install`` to add a driver, and ``dbc list`` to see what is available:

.. code-block:: bash

   # Install a driver for a specific database
   dbc install postgresql
   dbc install snowflake
   dbc install sqlite

   # List available and installed drivers
   dbc list

After a driver is installed, ADBC client libraries (and tools like DuckDB's ``adbc`` extension) can load it automatically by name.

See the `dbc documentation <https://docs.columnar.tech/dbc>`_ for the full list of available drivers and advanced usage.

----

DuckDB
======

`DuckDB <https://duckdb.org>`_ integrates with ADBC in two ways:

1. **DuckDB as an ADBC driver** — DuckDB exposes an ADBC interface so you can connect to a DuckDB database from any ADBC client library.
2. **DuckDB as an ADBC client** — DuckDB's community ``adbc`` extension lets you query *other* databases (PostgreSQL, Snowflake, BigQuery, etc.) directly from SQL using ADBC drivers.

See :doc:`driver/duckdb` for details on using DuckDB as an ADBC driver.

Using the DuckDB ``adbc`` extension
------------------------------------

The `adbc extension <https://duckdb.org/community_extensions/extensions/adbc.html>`_ (available for DuckDB 1.4.5+ and 1.5.4+) allows DuckDB to read from and write to any database that has an ADBC driver:

.. code-block:: sql

   INSTALL adbc FROM community;
   LOAD adbc;

   -- Read from an external database using a connection profile
   SELECT * FROM read_adbc('profile://my_postgres', 'SELECT * FROM orders');

   -- Attach an external database as a DuckDB catalog
   ATTACH 'profile://my_postgres' AS pg (TYPE adbc);
   SELECT * FROM pg.public.orders;

   -- Execute arbitrary DDL/DML on the remote database
   CALL adbc_execute('profile://my_postgres', 'TRUNCATE staging.temp_load');

Connections are specified using ADBC :doc:`connection profiles <connection_profiles>`. Drivers are installed with :ref:`dbc <dbc>`.

For details on the ``adbc_scanner`` functionality, see the `adbc_scanner documentation <https://duckdb.org/community_extensions/extensions/adbc_scanner>`_.

----

Go ``database/sql``
===================

The Go ADBC client library provides an adapter so ADBC drivers can be used through Go's standard `database/sql <https://pkg.go.dev/database/sql>`_ interface. This means you can use ADBC drivers with any library or framework that accepts a ``*sql.DB``.

.. code-block:: go

   import (
       "database/sql"
       "github.com/apache/arrow-adbc/go/adbc/sqldriver"
       "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
   )

   // Register the ADBC driver as a database/sql driver
   sql.Register("flightsql", sqldriver.Driver{Driver: flightsql.NewDriver()})

   db, err := sql.Open("flightsql", "grpc://localhost:32010")

See the `Go documentation <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`_ for full details.

----

pandas
======

`pandas <https://pandas.pydata.org>`_ can read query results from any ADBC driver via the ``adbc_driver_manager`` package. Because ADBC transfers data as Arrow, the conversion to a pandas ``DataFrame`` is efficient and avoids unnecessary copies.

.. code-block:: python

   import adbc_driver_postgresql.dbapi as pg
   import pandas as pd

   with pg.connect("postgresql://localhost:5432/mydb") as conn:
       df = pd.read_sql("SELECT * FROM orders", conn)

ADBC connections are compatible with ``pd.read_sql``, ``pd.read_sql_query``, and ``pd.read_sql_table``.

See the recipe :ref:`Using Pandas and ADBC <recipe-postgresql-statement-nocopy>` for more examples.

----

Polars
======

`Polars <https://pola.rs>`_ has native ADBC support via ``read_database`` and ``write_database``. ADBC provides zero-copy Arrow transfer between Polars and the database.

.. code-block:: python

   import polars as pl

   # Read from a database
   df = pl.read_database(
       query="SELECT * FROM orders",
       connection="postgresql://localhost:5432/mydb",
       engine="adbc",
   )

   # Write to a database
   df.write_database(
       table_name="orders_copy",
       connection="postgresql://localhost:5432/mydb",
       engine="adbc",
   )

See the `Polars database documentation <https://docs.pola.rs/user-guide/io/database/>`_ for more details.

----

dplyr
=====

In R, `dplyr <https://dplyr.tidyverse.org>`_ accesses databases through the `DBI <https://dbi.r-dbi.org>`_ interface. The ``adbcdrivermanager`` package provides a DBI-compatible backend so you can use any ADBC driver with dplyr and ``dbplyr``.

.. code-block:: r

   library(adbcdrivermanager)
   library(dplyr)

   con <- adbc_driver("postgresql") |>
     adbc_database_init(uri = "postgresql://localhost:5432/mydb") |>
     adbc_connection_init() |>
     as_adbc_dbi_connection()

   orders <- tbl(con, "orders") |>
     filter(status == "shipped") |>
     collect()

----

GDAL
====

`GDAL <https://gdal.org>`_ (Geospatial Data Abstraction Library) supports reading and writing spatial data from databases via ADBC through its `ADBC OGR driver <https://gdal.org/en/stable/drivers/vector/adbc.html>`_. This lets you use any ADBC-compatible database as a vector data source.

.. code-block:: bash

   # List layers in a PostGIS database
   ogrinfo ADBC:postgresql://localhost:5432/geodata

   # Convert a layer to GeoJSON
   ogr2ogr output.geojson ADBC:postgresql://localhost:5432/geodata my_spatial_table

See the `GDAL ADBC driver documentation <https://gdal.org/en/stable/drivers/vector/adbc.html>`_ for full details.

----

Add your own here!
==================

Using ADBC with a tool not listed here? Contributions to this page are welcome — open a pull request on `GitHub <https://github.com/apache/arrow-adbc>`_.
