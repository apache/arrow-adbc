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

=======================
Drivers (Unified Table)
=======================

.. |click-for-docs| raw:: html

   <span class="th-hint">(click for docs)</span>

.. |click-for-readme| raw:: html

   <span class="th-hint">(click for README)</span>

An ADBC driver lets you connect to a database, run queries, and exchange data in
Arrow format.

.. _driver-table:

Available drivers
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
   * - `Google BigQuery <https://docs.adbc-drivers.org/drivers/bigquery/>`__
     - ``bigquery``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/bigquery|GitHub repository`
   * - `ClickHouse <https://docs.adbc-drivers.org/drivers/clickhouse/>`__
     - ``clickhouse``
     - Vendor
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/clickhouse|GitHub repository`
   * - `Databricks <https://docs.adbc-drivers.org/drivers/databricks/>`__
     - ``databricks``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/databricks|GitHub repository`
   * - `Apache DataFusion <https://docs.adbc-drivers.org/drivers/datafusion/>`__
     - ``datafusion``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/datafusion|GitHub repository`
   * - :doc:`DuckDB <duckdb>` [#duckdb]_
     - ``duckdb``
     - Vendor
     - :iconlink:`fa-brands fa-github|https://github.com/duckdb/duckdb|GitHub repository`
   * - `Exasol <https://docs.adbc-drivers.org/drivers/exasol/>`__
     - ``exasol``
     - Vendor
     - :iconlink:`fa-brands fa-github|https://github.com/exasol/adbc-driver-exasol|GitHub repository`
   * - :doc:`Apache Arrow Flight SQL <flight_sql>`
     - ``flightsql``
     - ASF
     - :iconlink:`fa-brands fa-github|https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/flightsql|GitHub repository`
   * - `Microsoft SQL Server <https://docs.adbc-drivers.org/drivers/mssql/>`__
     - ``mssql``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/mssql|GitHub repository`
   * - `MySQL <https://docs.adbc-drivers.org/drivers/mysql/>`__
     - ``mysql``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/mysql|GitHub repository`
   * - :doc:`PostgreSQL <postgresql>`
     - ``postgresql``
     - ASF
     - :iconlink:`fa-brands fa-github|https://github.com/apache/arrow-adbc/tree/main/c/driver/postgresql|GitHub repository`
   * - `Amazon Redshift <https://docs.adbc-drivers.org/drivers/redshift/>`__
     - ``redshift``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/redshift|GitHub repository`
   * - `Snowflake <https://docs.adbc-drivers.org/drivers/snowflake/>`__
     - ``snowflake``
     - Foundry
     - :iconlink:`fa-brands fa-github|https://github.com/adbc-drivers/snowflake|GitHub repository`
   * - :doc:`SQLite <sqlite>`
     - ``sqlite``
     - ASF
     - :iconlink:`fa-brands fa-github|https://github.com/apache/arrow-adbc/tree/main/c/driver/sqlite|GitHub repository`
   * - `Trino <https://docs.adbc-drivers.org/drivers/trino/>`__
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
.. [#maintainer] See :ref:`Who maintains these drivers? <driver-table-maintainers>`
   for what **ASF**, **Foundry**, and **Vendor** mean.
.. [#duckdb] ADBC support is built directly into DuckDB; installing the
   ``duckdb`` driver fetches a prebuilt binary.

.. _driver-table-install:

Installing drivers
==================

Prebuilt driver binaries for the drivers above—code-signed, notarized, and
ready to install for most mainstream operating systems and CPU
architectures—are available from the `ADBC Driver Registry
<https://dbc-cdn.columnar.tech>`__.

Users and applications can download a driver from the registry manually, or with
the `dbc <https://docs.columnar.tech/dbc/>`__ command-line tool. With dbc
installed, install any driver by its slug:

.. code-block:: shell

   dbc install <slug>       # e.g. dbc install postgresql

dbc installs both the driver's shared library and its
:term:`driver manifest` to a location where an ADBC client library can find the
driver by its slug. If you download a driver from the registry manually instead,
you'll need to install the shared library to the correct location yourself and
create a driver manifest for it, or else pass the full path to the shared
library in your application code, plus the entrypoint if the driver does not
use the default entrypoint name.
See :doc:`ADBC Driver Manager and Manifests <../format/driver_manifests>` for
details.

Some drivers are also published to language-specific package registries.
Apache-maintained drivers are on
:doc:`PyPI, conda-forge, CRAN, and more <installation>`; some vendor
drivers list their own packages in each driver's documentation, linked from its
name in the table above.

.. note::
   The `ADBC Driver Registry <https://dbc-cdn.columnar.tech/>`__ is a
   community-maintained registry of prebuilt drivers, and
   `dbc <https://docs.columnar.tech/dbc/>`__ is an independent command-line tool
   for installing them. Neither is a part of the Apache Arrow project.

.. _driver-table-language-specific:

Language-specific drivers
=========================

Some drivers are built and packaged for a specific language or runtime rather
than as dynamically loadable shared libraries. They run directly on that runtime,
without crossing an FFI / ABI boundary, and are distributed through its package
registry. They can be used from any language that targets that runtime — for
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
     - Maintainer [#maintainer]_
   * - `Apache Arrow Flight SQL <https://github.com/apache/arrow-adbc/tree/main/csharp/src/Drivers/FlightSql/README.md>`__
     - :package-badge:`NuGet|Apache.Arrow.Adbc.FlightSql|https://www.nuget.org/packages/Apache.Arrow.Adbc.FlightSql/`
     - ASF
   * - `Google BigQuery <https://github.com/apache/arrow-adbc/blob/main/csharp/src/Drivers/BigQuery/readme.md>`__
     - :package-badge:`NuGet|Apache.Arrow.Adbc.Drivers.BigQuery|https://www.nuget.org/packages/Apache.Arrow.Adbc.Drivers.BigQuery/`
     - ASF
   * - `Apache Hive / Impala / Spark <https://github.com/apache/arrow-adbc/blob/main/csharp/src/Drivers/Apache/readme.md>`__
     - :package-badge:`NuGet|Apache.Arrow.Adbc.Drivers.Apache|https://www.nuget.org/packages/Apache.Arrow.Adbc.Drivers.Apache/`
     - ASF
   * - `Databricks <https://github.com/apache/arrow-adbc/blob/main/csharp/src/Drivers/Databricks/readme.md>`__
     - :package-badge:`NuGet|Apache.Arrow.Adbc.Drivers.Databricks|https://www.nuget.org/packages/Apache.Arrow.Adbc.Drivers.Databricks/`
     - ASF

.. _driver-table-java:

Java
----

Distributed as Maven packages, for use from Java applications:

.. list-table::
   :header-rows: 1
   :class: driver-table

   * - Driver for |click-for-docs|
     - Maven package
     - Maintainer [#maintainer]_
   * - `Apache Arrow Flight SQL <https://arrow.apache.org/adbc/current/java/api/org/apache/arrow/adbc/driver/flightsql/package-summary.html>`__
     - :package-badge:`Maven|org.apache.arrow.adbc:adbc-driver-flight-sql|https://central.sonatype.com/artifact/org.apache.arrow.adbc/adbc-driver-flight-sql`
     - ASF

.. _driver-table-maintainers:

Who maintains these drivers?
============================

The ADBC project focuses primarily on the :doc:`ADBC standard <../format/specification>`
and :doc:`client libraries <../client_libraries>`, not on developing drivers
and shipping driver binaries. The **Maintainer** column in the table above
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

Have a driver to add?
=====================

If you've developed an ADBC driver and would like it listed here,
`open an issue <https://github.com/apache/arrow-adbc/issues/new>`__ on the
Apache Arrow ADBC repository with the details (name, database, repository URL,
package names) and we'll help get it added.

If you're interested in developing a new driver, consider reaching out to the
`ADBC Driver Foundry <https://github.com/adbc-drivers/>`__ and see their
`Building Drivers <https://docs.adbc-drivers.org/building-drivers/>`__ guide.

More information
================

- :doc:`Client Libraries <../client_libraries>` — the libraries that load and use drivers from your language of choice
- :doc:`Tools & Integrations <../integrations>` — tools and libraries that work with ADBC
- :doc:`Connection Profiles <../connection_profiles>` — reusable, shareable connection configuration
- `ADBC Quickstarts <https://github.com/columnar-tech/adbc-quickstarts>`__ — simple, runnable examples for getting started with many of these drivers
