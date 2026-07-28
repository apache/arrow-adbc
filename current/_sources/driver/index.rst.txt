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

=======
Drivers
=======

.. |click-for-docs| raw:: html

   <span class="th-hint">(click for docs)</span>

.. |click-for-readme| raw:: html

   <span class="th-hint">(click for README)</span>

An ADBC driver lets you connect to a database, run queries, and exchange data in
Arrow format.

.. _driver-table:

Available Drivers
=================

ADBC drivers are typically built, distributed, and used as **shared libraries**
(``.so``, ``.dll``, or ``.dylib`` files). One shared library works from any
language. An application uses an
:doc:`ADBC client library <../client_libraries>` to dynamically load the driver
at runtime, no matter which language the driver was written in. Every
driver in the table below is packaged this way.

.. list-table::
   :header-rows: 1
   :widths: 38 22 22 18
   :class: driver-table driver-table-main

   * - Driver for |click-for-docs|
     - Slug [#slug]_
     - Maintainer [#maintainer]_
     - Repo
   * - `BigQuery <https://adbc-drivers.org/drivers/bigquery/>`__
     - ``bigquery``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/bigquery|GitHub repository`
   * - `ClickHouse <https://adbc-drivers.org/drivers/clickhouse/>`__
     - ``clickhouse``
     - Vendor
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/clickhouse|GitHub repository`
   * - `Databricks <https://adbc-drivers.org/drivers/databricks/>`__
     - ``databricks``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/databricks|GitHub repository`
   * - `Apache DataFusion <https://adbc-drivers.org/drivers/datafusion/>`__
     - ``datafusion``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/datafusion|GitHub repository`
   * - :doc:`DuckDB <duckdb>` [#duckdb]_
     - ``duckdb``
     - Vendor
     - :iconlink:`fa-brands fa-github|https://github.com/duckdb/duckdb|GitHub repository`
   * - `Exasol <https://adbc-drivers.org/drivers/exasol/>`__
     - ``exasol``
     - Vendor
     - :iconlink:`fa-brands fa-github|https://github.com/exasol/adbc-driver-exasol|GitHub repository`
   * - :doc:`Apache Arrow Flight SQL <flight_sql>` [#compat-flightsql]_
     - ``flightsql``
     - ASF
     - :iconlink:`fa-brands fa-github|https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/flightsql|GitHub repository`
   * - `Microsoft SQL Server <https://adbc-drivers.org/drivers/mssql/>`__
     - ``mssql``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/mssql|GitHub repository`
   * - `MySQL/MariaDB <https://adbc-drivers.org/drivers/mysql/>`__ [#compat-mysql]_
     - ``mysql``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/mysql|GitHub repository`
   * - :doc:`PostgreSQL <postgresql>` [#compat-postgresql]_
     - ``postgresql``
     - ASF
     - :iconlink:`fa-brands fa-github|https://github.com/apache/arrow-adbc/tree/main/c/driver/postgresql|GitHub repository`
   * - `DuckDB Quack <https://adbc-drivers.org/drivers/quack/>`__
     - ``quack``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/quack|GitHub repository`
   * - `Amazon Redshift <https://adbc-drivers.org/drivers/redshift/>`__
     - ``redshift``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/redshift|GitHub repository`
   * - `SingleStore <https://adbc-drivers.org/drivers/singlestore/>`__
     - ``singlestore``
     - Vendor
     - :iconlink:`fa-brands fa-github|https://github.com/singlestore-labs/singlestore-adbc-connector|GitHub repository`
   * - `Snowflake <https://adbc-drivers.org/drivers/snowflake/>`__
     - ``snowflake``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/snowflake|GitHub repository`
   * - `Apache Spark <https://adbc-drivers.org/drivers/spark/>`__
     - ``spark``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/spark|GitHub repository`
   * - :doc:`SQLite <sqlite>`
     - ``sqlite``
     - ASF
     - :iconlink:`fa-brands fa-github|https://github.com/apache/arrow-adbc/tree/main/c/driver/sqlite|GitHub repository`
   * - `Trino <https://adbc-drivers.org/drivers/trino/>`__
     - ``trino``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/trino|GitHub repository`

.. [#slug] The **slug** is the short name a driver is known by. It is the name
   you hand to a :doc:`client library <../client_libraries>` to load the
   driver—either as the driver name (``postgresql``) or as the URI scheme
   (``postgresql://``). Given this name, the client library
   :ref:`searches for a matching driver manifest <driver-manifest-discovery>`
   (``postgresql.toml``) that tells it which shared library to load. The same
   slug is what you pass when :ref:`installing a driver <driver-table-install>`.
.. [#maintainer] See :ref:`Who Maintains These Drivers? <driver-table-maintainers>`
   for what **ASF**, **Foundry**, and **Vendor** mean.
.. [#duckdb] ADBC support is built directly into DuckDB; installing the
   ``duckdb`` driver fetches a prebuilt binary. This driver is also compatible
   with MotherDuck.
.. [#compat-flightsql] This driver works with Flight SQL-compatible systems,
   including Apache Doris, Dremio, GizmoSQL, InfluxDB, Sail, and StarRocks.
.. [#compat-mysql] This driver also works with MySQL-compatible systems including
   TiDB and Vitess.
.. [#compat-postgresql] This driver also works with PostgreSQL wire
   protocol-compatible systems, including CedarDB, Citus, CockroachDB, CrateDB,
   Neon, ParadeDB, TimescaleDB, Yellowbrick, and YugabyteDB.

.. _driver-table-install:

Installing Drivers
==================

Pre-built binaries for the drivers in the table above are available from the
`ADBC driver registry <https://dbc-cdn.columnar.tech>`__ hosted by
`Columnar <https://columnar.tech>`__, a company active in ADBC driver
development. Binaries are available for Windows (amd64), macOS (arm64), and
Linux (amd64 and arm64), and are code-signed and notarized by Columnar.

Users and applications can download a driver from the registry manually, or with
the `dbc <https://docs.columnar.tech/dbc/>`__ command-line tool. With dbc
installed, install any driver by its slug:

.. code-block:: shell

   dbc install <slug>       # e.g. dbc install postgresql

dbc installs both the driver's shared library and its
:term:`driver manifest` to a location where an ADBC client library can find the
driver by its slug.

If you download a driver from the registry manually instead, you'll need to
install the driver shared library file to the correct location yourself and
create a driver manifest for it, or else pass the full path to the shared
library in your application code, plus the entrypoint if the driver does not
use the default entrypoint name.
See :doc:`ADBC Driver Manager and Manifests <../format/driver_manifests>` for
details.

Some drivers are also published to language-specific package registries.
Apache-maintained drivers are on
:ref:`PyPI, conda-forge, CRAN, and more <driver-releases>`; some vendor
drivers list their own packages in each driver's documentation, linked from its
name in the table above.

.. note::
   The `ADBC driver registry <https://dbc-cdn.columnar.tech/>`__ and the
   `dbc <https://docs.columnar.tech/dbc/>`__ command-line tool for installing
   drivers from it are provided by Columnar, and are maintained independently of
   the Apache Arrow project.

.. _driver-table-language-specific:

Language-Specific Drivers
=========================

Some drivers are built and packaged for a specific language or runtime rather
than as dynamically loadable shared libraries. They run directly on that runtime,
without crossing an FFI / ABI boundary, and are distributed through its package
registry. They can be used from any language that targets that runtime. For
example, the .NET drivers work from C#, F#, and other .NET languages, and the
Java driver works from Java, Kotlin, Scala, and other JVM languages.

.. _driver-table-csharp:

C#/.NET
-------

Distributed as .NET packages on NuGet, for use from C#/.NET applications:

.. list-table::
   :header-rows: 1
   :class: driver-table

   * - Driver for |click-for-readme|
     - NuGet package
     - Maintainer
   * - `Apache Arrow Flight SQL <https://github.com/apache/arrow-adbc/tree/main/csharp/src/Drivers/FlightSql/README.md>`__
     - :package-badge:`NuGet|Apache.Arrow.Adbc.FlightSql|https://www.nuget.org/packages/Apache.Arrow.Adbc.FlightSql/`
     - ASF
   * - :doc:`Google BigQuery <./bigquery>`
     - :package-badge:`NuGet|Apache.Arrow.Adbc.Drivers.BigQuery|https://www.nuget.org/packages/Apache.Arrow.Adbc.Drivers.BigQuery/`
     - ASF
   * - `Apache Hive / Impala / Spark <https://github.com/apache/arrow-adbc/blob/main/csharp/src/Drivers/Apache/readme.md>`__
     - :package-badge:`NuGet|Apache.Arrow.Adbc.Drivers.Apache|https://www.nuget.org/packages/Apache.Arrow.Adbc.Drivers.Apache/`
     - ASF
   * - `Databricks <https://github.com/apache/arrow-adbc/blob/main/csharp/src/Drivers/Databricks/readme.md>`__
     - :package-badge:`NuGet|Apache.Arrow.Adbc.Drivers.Databricks|https://www.nuget.org/packages/Apache.Arrow.Adbc.Drivers.Databricks/`
     - ASF

.. _driver-table-java:

Java/JVM
--------

Distributed as Maven packages, for use from Java applications:

.. list-table::
   :header-rows: 1
   :class: driver-table

   * - Driver for |click-for-docs|
     - Maven package
     - Maintainer
   * - `Apache Arrow Flight SQL <https://arrow.apache.org/adbc/current/java/api/org/apache/arrow/adbc/driver/flightsql/package-summary.html>`__
     - :package-badge:`Maven|org.apache.arrow.adbc:adbc-driver-flight-sql|https://central.sonatype.com/artifact/org.apache.arrow.adbc/adbc-driver-flight-sql`
     - ASF
   * - :doc:`./jdbc`
     - :package-badge:`Maven|org.apache.arrow.adbc:adbc-driver-jdbc|https://central.sonatype.com/artifact/org.apache.arrow.adbc/adbc-driver-jdbc`
     - ASF

.. _driver-table-maintainers:

Who Maintains These Drivers?
============================

The ADBC project focuses primarily on the :doc:`ADBC standard <../format/specification>`
and :doc:`client libraries <../client_libraries>`, not on developing drivers
and shipping driver binaries. The **Maintainer** column in the tables above
identifies who maintains each driver:

**ASF**
   A few drivers are maintained within the Apache Arrow project under
   Apache Software Foundation (ASF) governance, mostly for historical reasons.

**Foundry**
   Most drivers are maintained within the `ADBC Driver Foundry <https://adbc-drivers.org/>`__,
   a community-governed project focused on growing the ADBC ecosystem.

**Vendor**
   Some drivers are maintained in independent repositories controlled
   by database vendors.

Have a Driver to Add?
=====================

If you've developed a high-quality ADBC driver, made it freely available for
public use under a permissive license, and would like it listed here,
`open an issue <https://github.com/apache/arrow-adbc/issues/new>`__ on the
Apache Arrow ADBC repository with the details (name, database, repository URL,
package names) and we'll help get it added.

Interested in developing a new driver? See :doc:`Writing New Drivers <authoring>`.

More Information
================

- :doc:`Client Libraries <../client_libraries>` — the libraries that load and use drivers from your language of choice
- :doc:`Tools & Integrations <../integrations>` — tools and libraries that work with ADBC
- :doc:`Connection Profiles <../connection_profiles>` — reusable, shareable connection configuration
- `ADBC Quickstarts <https://github.com/columnar-tech/adbc-quickstarts>`__ — simple, runnable examples for getting started with many of these drivers
