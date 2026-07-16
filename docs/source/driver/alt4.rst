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

The ADBC project is primarily focused on developing the :doc:`ADBC standard <../format/specification>` and :doc:`client libraries <../client_libraries>` and not on building and distributing driver binaries. While some driver binaries are available directly from the project, many more driver binaries are available from :ref:`third parties<driver-index-third-party>`.

.. _driver-index-apache:

Apache Drivers
==============

The following drivers are developed and published by the ADBC project:

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


.. _driver-index-third-party:

Third-Party Drivers
===================

.. note::
   Any projects linked in this section are not part of the Apache Software Foundation (ASF) and any software distributed by them should not be considered official products of the ASF.

The majority of ADBC drivers are available from various third parties:

- `DuckDB <https://duckdb.org/docs/api/adbc>`__: ADBC support is built directly into DuckDB.
- `ADBC Driver Foundry <https://docs.adbc-drivers.org/>`__: Community-governed project focused on growing the ADBC ecosystem.

**Have a driver to add?**

If you've developed an ADBC driver and would like it listed in the documentation:

1. `Open an issue <https://github.com/apache/arrow-adbc/issues/new>`__ on the Apache Arrow ADBC repository with details about your driver (name, database, language, repository URL, package names)
2. Or submit a pull request directly to add your driver to this page

If you're interested in developing a new driver, we recommend reaching out to the `ADBC Driver Foundry <https://github.com/adbc-drivers/>`__.

Installing Drivers
==================

Apache Driver Packages
----------------------

Apache ADBC driver packages are available on the following platforms:

- **PyPI** (Python): ``adbc-driver-flightsql``, ``adbc-driver-postgresql``, ``adbc-driver-sqlite``
- **conda-forge** (Python): ``adbc-driver-flightsql``, ``adbc-driver-postgresql``, ``adbc-driver-sqlite``
- **conda-forge** (C/C++ libraries): ``adbc-driver-flightsql-go``, ``adbc-driver-manager-cpp``, ``adbc-driver-postgresql-cpp``, ``adbc-driver-sqlite-cpp``
- **CRAN** (R): ``adbcpostgresql``, ``adbcsqlite``
- **R-multiverse** (R): ``adbcflightsql``
- **Maven Central** (Java): ``org.apache.arrow.adbc:adbc-driver-flight-sql``, ``org.apache.arrow.adbc:adbc-driver-jdbc``
- **NuGet** (C#): ``Apache.Arrow.Adbc.Drivers.FlightSql``, ``Apache.Arrow.Adbc.Drivers.BigQuery``, ``Apache.Arrow.Adbc.Drivers.Apache`` (Hive/Impala/Spark/Databricks), ``Apache.Arrow.Adbc.Drivers.Interop.FlightSql``
- **npm** (JavaScript): ``@apache-arrow/adbc-driver-manager`` (with platform-specific packages: ``-darwin-arm64``, ``-darwin-x64``, ``-linux-arm64-gnu``, ``-linux-x64-gnu``, ``-win32-x64-msvc``)
- **crates.io** (Rust): ``adbc_core``, ``adbc_driver_manager``, ``adbc_ffi``
- **RubyGems** (Ruby): ``red-adbc``
- **APT/DNF** (C/C++): ``libadbc-driver-flightsql-dev``, ``libadbc-driver-postgresql-dev``, ``libadbc-driver-sqlite-dev``

Or by driver:

.. list-table::
   :header-rows: 1

   * - Driver
     - Python (PyPI / conda-forge)
     - R
     - Java (Maven Central) [#pkg-groupid]_
     - C# (NuGet) [#pkg-csharp]_
     - C/C++ (conda-forge)
     - C/C++ (APT/DNF)
   * - Apache Arrow Flight SQL
     - ``adbc-driver-flightsql``
     - ``adbcflightsql`` [#pkg-rmultiverse]_
     - ``adbc-driver-flight-sql``
     - ``Apache.Arrow.Adbc.Drivers.FlightSql``
     - ``adbc-driver-flightsql-go``
     - ``libadbc-driver-flightsql-dev``
   * - Apache Arrow Flight SQL (Interop)
     - —
     - —
     - —
     - ``Apache.Arrow.Adbc.Drivers.Interop.FlightSql``
     - —
     - —
   * - BigQuery
     - —
     - —
     - —
     - ``Apache.Arrow.Adbc.Drivers.BigQuery``
     - —
     - —
   * - Apache Hive/Impala/Spark/Databricks
     - —
     - —
     - —
     - ``Apache.Arrow.Adbc.Drivers.Apache``
     - —
     - —
   * - JDBC
     - —
     - —
     - ``adbc-driver-jdbc``
     - —
     - —
     - —
   * - PostgreSQL
     - ``adbc-driver-postgresql``
     - ``adbcpostgresql``
     - —
     - —
     - ``adbc-driver-postgresql-cpp``
     - ``libadbc-driver-postgresql-dev``
   * - SQLite
     - ``adbc-driver-sqlite``
     - ``adbcsqlite``
     - —
     - —
     - ``adbc-driver-sqlite-cpp``
     - ``libadbc-driver-sqlite-dev``

.. [#pkg-groupid] Group ID: ``org.apache.arrow.adbc``
.. [#pkg-csharp] Group ID: ``Apache.Arrow.Adbc.Drivers``
.. [#pkg-rmultiverse] Available from R-multiverse, not CRAN

See the :doc:`installation` page for full details.

Client Library and Driver Manager Packages
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

In addition to driver packages, the following client library and driver manager packages are available:

.. list-table::
   :header-rows: 1

   * - Language
     - Package Manager
     - Package Name(s)
   * - Python
     - PyPI / conda-forge
     - ``adbc-driver-manager``
   * - C/C++
     - conda-forge
     - ``adbc-driver-manager-cpp``
   * - C#/.NET
     - NuGet
     - ``Apache.Arrow.Adbc``, ``Apache.Arrow.Adbc.Client``
   * - Go
     - Go modules
     - ``github.com/apache/arrow-adbc/go/adbc``
   * - Java
     - Maven Central
     - ``org.apache.arrow.adbc:adbc-driver-manager``, ``org.apache.arrow.adbc:adbc-core``
   * - JavaScript
     - npm
     - ``@apache-arrow/adbc-driver-manager``
   * - R
     - CRAN
     - ``adbcdrivermanager``
   * - Ruby
     - RubyGems
     - ``red-adbc``
   * - Rust
     - crates.io
     - ``adbc_core``, ``adbc_driver_manager``, ``adbc_ffi``

Third-Party Packages
--------------------

.. note:: Columnar is not part of the Apache Software Foundation and `dbc` is not an official Apache Software Foundation project.

Packages for many of the official and third-party drivers are available from `Columnar <https://columnar.tech>`__ and can be installed with their CLI tool, `dbc <https://docs.columnar.tech/dbc>`__.

.. button-link:: https://columnar.tech/dbc
  :color: primary

  Learn about ``dbc`` :octicon:`cross-reference`

.. _driver-index-language:

Driver Language
===============

You may notice in the table at the top of this page that some drivers have been implemented in multiple languages.
While ADBC was designed to make it possible to use a driver written in one language with a :doc:`client library <..//client_libraries>` written in any other language, there are some good reasons why a driver may get implemented multiple times and in different languages:

1. To take advantage of language-specific runtime features. Example: Apache Arrow Flight SQL's Java and C# implementations can take advantage of features of those platforms such as memory management, JIT compilation, concurrency mechanisms, amongst others.
2. Wrapped SDKs: Some ADBC drivers wrap official SDKs for the target database and the language the best SDK for a particular database is written in can change over time. ADBC's C ABI makes rewriting a driver in another language a stable experience for the user.
3. Language preference: ADBC's C ABI gives developers the freedom to write drivers in their language of choice.

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

Migrated Apache Drivers
=======================

A number of drivers were previously published from the ADBC project but have since been migrated to the `ADBC Driver Foundry <https://github.com/adbc-drivers>`__, where development continues. Packages on platforms such as PyPI and Conda Forge are still available but are not being updated.

.. list-table::
   :header-rows: 1

   * - Driver
     - Implementation
     - New Repository
     - Deprecated Packages
   * - Databricks
     - Go
     - `github.com/adbc-drivers/databricks <https://github.com/adbc-drivers/databricks>`__
     - Go: ``github.com/apache/arrow-adbc/go/adbc/driver/databricks``
   * - Google BigQuery
     - Go
     - `github.com/adbc-drivers/bigquery <https://github.com/adbc-drivers/bigquery>`__
     - | **Go**: ``github.com/apache/arrow-adbc/go/adbc/driver/bigquery``
       | **Python**: ``adbc-driver-bigquery`` (PyPI, conda-forge)
       | **R**: ``adbcbigquery`` (R-multiverse)
       | **C/C++**: ``libadbc-driver-bigquery`` (conda-forge, APT/DNF)
   * - Snowflake
     - Go
     - `github.com/adbc-drivers/snowflake <https://github.com/adbc-drivers/snowflake>`__
     - | **Go**: ``github.com/apache/arrow-adbc/go/adbc/driver/snowflake``
       | **Python**: ``adbc-driver-snowflake`` (PyPI, conda-forge)
       | **R**: ``adbcsnowflake`` (R-multiverse)
       | **Rust**: ``adbc_driver_snowflake`` (crates.io)
       | **C/C++**: ``libadbc-driver-snowflake`` (conda-forge, APT/DNF)
       | **C#**: ``Apache.Arrow.Adbc.Drivers.Interop.Snowflake`` (NuGet)
   * - DataFusion
     - Rust
     - Removed [#pkg-datafusion]_
     - **Rust**: ``adbc_driver_datafusion`` (crates.io)

.. [#pkg-datafusion] The DataFusion driver was removed as of ADBC 0.24.0

These deprecated packages are no longer maintained by the Apache Arrow ADBC project. Users should migrate to the ADBC Driver Foundry versions or use alternative drivers.

Alternative Views
=================

The driver information on this page can also be viewed in alternative formats:

- :doc:`Cards View <alt1>` - Visual grid layout with cards for each database
- :doc:`Database Sections <alt2>` - Detailed sections with installation tables
- :doc:`Unified Table <alt3>` - Single scannable table of all drivers
