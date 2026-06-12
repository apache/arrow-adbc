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

To connect to a database with ADBC, you need a :doc:`client library <..//client_libraries>` and a driver for your database.
This page aims to cover what ADBC drivers are available and links to learn more about them.

Official Drivers
================

The following drivers are developed inside the ADBC project and can be considered official:

.. list-table::
   :header-rows: 1

   * - Database/Vendor
     - :ref:`Language <driver-index-language>`
     - :ref:`Status <driver-index-status>`
     - :ref:`Driver Manager Loadable <driver-index-manager>`
   * - Apache Arrow Flight SQL
     - C#
     - Beta
     - No
   * - :doc:`Apache Arrow Flight SQL <flight_sql>`
     - Java
     - Beta
     - No
   * - :doc:`Apache Arrow Flight SQL <flight_sql>`
     - Go
     - Stable
     - Yes
   * - Apache DataFusion
     - Rust
     - Experimental
     - Yes
   * - Apache Hive
     - C#
     - Experimental
     - No
   * - Apache Impala
     - C#
     - Experimental
     - No
   * - Apache Spark
     - C#
     - Experimental
     - No
   * - Databricks
     - C#
     - Experimental
     - No
   * - :doc:`Google BigQuery <bigquery>`
     - C#
     - Beta
     - No
   * - :doc:`JDBC <jdbc>`
     - Java
     - Beta
     - No
   * - :doc:`PostgreSQL <postgresql>`
     - C/C++
     - Stable
     - Yes
   * - :doc:`SQLite <sqlite>`
     - C/C++
     - Stable
     - Yes


Third-Party Drivers
===================

.. note::
   Any projects linked in this section are not official Apache Software Foundation (ASF) projects and any software distributed by them should not be considered official products of the ASF.

Many more drivers are available outside of this project from a variety of sources:

- `DuckDB <https://arrow.apache.org/adbc/current/driver/duckdb.html>`__: ADBC support is built directly into DuckDB.
- `ADBC Driver Foundry <https://github.com/adbc-drivers/>`__: Community-governed project focused on growing the ADBC ecosystem.

If you've developed a driver or are interested in developing one, we recommend reaching out to the `ADBC Driver Foundry <https://github.com/adbc-drivers/>`__.

Installing Drivers
==================

.. note:: Columnar is not part of the Apache Software Foundation and dbc is not an official Apache Software Foundation project.

Packages for many of the official and third-party drivers are available from `Columnar <https://columnar.tech>`__ and can be installed with their CLI tool, `dbc <https://docs.columnar.tech/dbc>`__.

.. button-link:: https://columnar.tech/dbc
  :color: primary

  Learn about dbc :octicon:`cross-reference`

.. _driver-index-language:

Driver Language
===============

You may notice in the table at the top of this page that some drivers have been implemented in multiple languages.
While ADBC was designed to make it possible to use a driver written in one language with a :doc:`client library <..//client_libraries>` written in any other language, there are some good reasons why a driver may get implemented multiple times and in different languages:

1. To take advantage of langauge-specific runtime features. Example: Apache Arrow Flight SQL's Java and C# implementations can take advantage of features of those platforms such as memory management, JIT compilation, concurrency mechanisms, amongst others.
2. Wrapped SDKs: Some ADBC drivers wrap official SDKs for the target database and the language the best SDK for a particular database is written in can change over time. ADBC's C ABI makes rewriting a driver in another language a stable experience for the user.
3. Language preference: ADBC's C ABI gives developers the freedom to write drivers in their langauge of choice.

.. _driver-index-status:

Driver Status
=============

- **Experimental** drivers are not feature-complete and the implementation is still progressing.
- **Beta** drivers are (mostly) feature-complete but have only been available for a short time.
- **Stable** drivers are (mostly) feature-complete (as much as possible for the underlying database) and have been available/tested for a while.

.. _driver-index-manager:

Driver Manager Loadable
=======================

An :doc:`ADBC Driver Manager <..//format/how_manager>` dynamically loads a driver into an application at run-time and requires the driver to be built as a dynamic or shared library.

Retired Official Drivers
=========================

A number of drivers were previously published from the ADBC project but have since been migrated to the `ADBC Driver Foundry <https://github.com/adbc-driverse>`__, where development continues. Packages on platforms such as PyPI and Conda Forge are still available but are not being updated.

- Google BigQuery (Go): Migrated to `github.com/adbc-drivers/bigquery <https://github.com/adbc-drivers/bigquery>`__
- Databricks (Go): Migrated to `github.com/adbc-drivers/databricks <https://github.com/adbc-drivers/databricks>`__
- Snowflake (Go): Migrated to `github.com/adbc-drivers/snowflake <https://github.com/adbc-drivers/snowflake>`__

.. list-table::
   :header-rows: 1

   * - Driver
     - Implementation
     - New Repository
   * - Databricks
     - Go
     - `github.com/adbc-drivers/databricks <https://github.com/adbc-drivers/databricks>`__
   * - Google BigQuery
     - Go
     - `github.com/adbc-drivers/bigquery <https://github.com/adbc-drivers/bigquery>`__
   * - Snowflake
     - Go
     - `github.com/adbc-drivers/snowflake <https://github.com/adbc-drivers/snowflake>`__

!! TODO: Note retired packages

conda-forge (adbc-driver-bigquery, libadbc-driver-bigquery), PyPI, R-multiverse
conda-forge (adbc-driver-snowflake, libadbc-driver-snowflake), crates.io, Go, NuGet, PyPI, R-multiverse
