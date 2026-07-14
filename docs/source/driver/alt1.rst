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

============================
Drivers (Alternative View 1)
============================

ADBC drivers are available for many popular databases. Most drivers are developed and published by third parties, with a few maintained by the Apache Software Foundation for historical reasons.

.. note:: This is an alternative view of the :doc:`drivers page <index>`. The information is the same, just presented differently.

Available Drivers
=================

.. grid:: 1 2 2 3
   :gutter: 3
   :class-container: driver-cards

   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Amazon Redshift**
      ^^^

      **Written In**

      Go

      **Source**

      `adbc-drivers/redshift <https://github.com/adbc-drivers/redshift>`__

      **Packages**

      :package-badge:`dbc|redshift|https://dbc-cdn.columnar.tech`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Apache Arrow Flight SQL**
      ^^^

      **Written In**

      Go

      **Source**

      `apache/arrow-adbc <https://github.com/apache/arrow-adbc>`__

      **Packages**

      :package-badge:`dbc|flightsql|https://dbc-cdn.columnar.tech` :package-badge:`PyPI|adbc-driver-flightsql|https://pypi.org/project/adbc-driver-flightsql/` :package-badge:`Conda|adbc-driver-flightsql|https://anaconda.org/conda-forge/adbc-driver-flightsql` :package-badge:`R-universe|adbcflightsql|https://r-universe.dev/search/?q=adbcflightsql`

   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Apache Arrow Flight SQL**
      ^^^

      **Written In**

      Java

      **Source**

      `apache/arrow-adbc <https://github.com/apache/arrow-adbc>`__

      **Packages**

      :package-badge:`Maven|org.apache.arrow.adbc:adbc-driver-flight-sql|https://central.sonatype.com/artifact/org.apache.arrow.adbc/adbc-driver-flight-sql`

   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Apache Arrow Flight SQL**
      ^^^

      **Written In**

      C#

      **Source**

      `apache/arrow-adbc <https://github.com/apache/arrow-adbc>`__

      **Packages**

      :package-badge:`NuGet|Apache.Arrow.Adbc.FlightSql|https://www.nuget.org/packages/Apache.Arrow.Adbc.FlightSql/`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Apache DataFusion**
      ^^^

      **Written In**

      Rust

      **Source**

      `adbc-drivers/datafusion <https://github.com/adbc-drivers/datafusion>`__

      **Packages**

      :package-badge:`dbc|datafusion|https://dbc-cdn.columnar.tech`
      :package-badge:`crates.io|adbc_datafusion|https://crates.io/crates/adbc_datafusion`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **ClickHouse**
      ^^^

      **Written In**

      Go

      **Source**

      `ClickHouse/adbc-driver-clickhouse <https://github.com/ClickHouse/adbc-driver-clickhouse>`__

      **Packages**

      :package-badge:`dbc|clickhouse|https://dbc-cdn.columnar.tech`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Databricks**
      ^^^

      **Written In**

      Go

      **Source**

      `adbc-drivers/databricks <https://github.com/adbc-drivers/databricks>`__

      **Packages**

      :package-badge:`dbc|databricks|https://dbc-cdn.columnar.tech`
      :package-badge:`pip|adbc-driver-databricks|https://pypi.org/project/adbc-driver-databricks/`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **DuckDB**
      ^^^

      **Written In**

      C++

      **Docs:** `DuckDB ADBC Docs <https://duckdb.org/docs/api/adbc>`__

      **Source**

      `duckdb/duckdb <https://github.com/duckdb/duckdb>`__

      **Packages**

      ADBC support is built directly into DuckDB.

      :package-badge:`dbc|duckdb|https://dbc-cdn.columnar.tech`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Exasol**
      ^^^

      **Written In**

      Go

      **Source**

      `exasol/adbc-driver-exasol <https://github.com/exasol/adbc-driver-exasol>`__

      **Packages**

      :package-badge:`dbc|exasol|https://dbc-cdn.columnar.tech`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Google BigQuery (Go)**
      ^^^

      **Written In**

      Go

      **Source**

      `adbc-drivers/bigquery <https://github.com/adbc-drivers/bigquery>`__

      **Packages**

      :package-badge:`dbc|bigquery|https://dbc-cdn.columnar.tech` :package-badge:`PyPI|adbc-driver-bigquery|https://pypi.org/project/adbc-driver-bigquery/` :package-badge:`Conda|adbc-driver-bigquery|https://anaconda.org/conda-forge/adbc-driver-bigquery` :package-badge:`R-universe|adbcbigquery|https://r-universe.dev/search/?q=adbcbigquery`

   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Google BigQuery (C#)**
      ^^^

      **Written In**

      C#

      **Source**

      `apache/arrow-adbc <https://github.com/apache/arrow-adbc>`__

      **Packages**

      :package-badge:`NuGet|Apache.Arrow.Adbc.Drivers.BigQuery|https://www.nuget.org/packages/Apache.Arrow.Adbc.Drivers.BigQuery/`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Microsoft SQL Server**
      ^^^

      **Written In**

      Go

      **Packages**

      :package-badge:`dbc|mssql|https://dbc-cdn.columnar.tech`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **MySQL**
      ^^^

      **Written In**

      Go

      **Source**

      `adbc-drivers/mysql <https://github.com/adbc-drivers/mysql>`__

      **Packages**

      :package-badge:`dbc|mysql|https://dbc-cdn.columnar.tech`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **PostgreSQL**
      ^^^

      **Written In**

      C/C++

      **Source**

      `apache/arrow-adbc <https://github.com/apache/arrow-adbc>`__

      **Packages**

      :package-badge:`dbc|postgresql|https://dbc-cdn.columnar.tech`
      :package-badge:`pip|adbc-driver-postgresql|https://pypi.org/project/adbc-driver-postgresql/`
      :package-badge:`conda|adbc-driver-postgresql|https://anaconda.org/conda-forge/adbc-driver-postgresql`
      :package-badge:`CRAN|adbcpostgresql|https://cran.r-project.org/package=adbcpostgresql`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Snowflake**
      ^^^

      **Written In**

      Go

      **Source**

      `adbc-drivers/snowflake <https://github.com/adbc-drivers/snowflake>`__

      **Packages**

      :package-badge:`dbc|snowflake|https://dbc-cdn.columnar.tech` :package-badge:`PyPI|adbc-driver-snowflake|https://pypi.org/project/adbc-driver-snowflake/` :package-badge:`Conda|adbc-driver-snowflake|https://anaconda.org/conda-forge/adbc-driver-snowflake` :package-badge:`R-universe|adbcsnowflake|https://r-universe.dev/search/?q=adbcsnowflake`
   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **SQLite**
      ^^^

      **Written In**

      C/C++

      **Source**

      `apache/arrow-adbc <https://github.com/apache/arrow-adbc>`__

      **Packages**

      :package-badge:`dbc|sqlite|https://dbc-cdn.columnar.tech` :package-badge:`PyPI|adbc-driver-sqlite|https://pypi.org/project/adbc-driver-sqlite/` :package-badge:`Conda|adbc-driver-sqlite|https://anaconda.org/conda-forge/adbc-driver-sqlite` :package-badge:`CRAN|adbcsqlite|https://cran.r-project.org/package=adbcsqlite`

   .. grid-item-card::
      :class-header: sd-bg-secondary sd-text-white


      **Trino**
      ^^^

      **Written In**

      Go

      **Source**

      `adbc-drivers/trino <https://github.com/adbc-drivers/trino>`__

      **Packages**

      :package-badge:`dbc|trino|https://dbc-cdn.columnar.tech`

Driver Vendors
==============

ADBC drivers come from several sources:

- **Apache Software Foundation**: A few drivers (PostgreSQL, SQLite, Flight SQL) are maintained by the ASF for historical reasons
- **ADBC Driver Foundry**: A community-governed project focused on growing the ADBC ecosystem (`docs <https://docs.adbc-drivers.org/>`__)
- **Database Vendors**: Some databases like DuckDB and ClickHouse provide their own ADBC drivers

.. note:: Projects linked in this section are not part of the Apache Software Foundation. Columnar is not part of the Apache Software Foundation and `dbc` is not an official Apache Software Foundation project.

More Information
================

- :doc:`Driver Status <status>` - Detailed status and feature support for each driver
- :doc:`Installation Guide <installation>` - Complete installation instructions
- :doc:`Building Drivers <sdk>` - Learn how to build your own ADBC driver
