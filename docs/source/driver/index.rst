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

This page covers the drivers available both within the ADBC project and elsewhere in the ADBC ecosystem.
ADBC is designed so anyone can build, distribute, and use a new driver without needing to upstream it to the ADBC project.

Official Drivers
================

A small set of ADBC drivers are built and distributed directly from the ADBC project.
The reason for this is mostly historical.

- flightsql
- sqlite
- postgresql

!! TODO, make into a table I think

Third-Party Drivers
===================

.. note::
   Any projects linked in this section are not official Apache Software Foundation (ASF) projects and any software distributed by them should not be considered official products of the ASF.

Many more drivers are available outside of this project from a variety of sources:

- `DuckDB <https://arrow.apache.org/adbc/current/driver/duckdb.html>`__: DuckDB (technically libduckdb) is an ADBC driver itself and can be loaded by an ADBC Driver Manager.
- `ADBC Driver Foundry <https://github.com/adbc-drivers/>`__: Community-governed project focused on growing the ADBC ecosystem.
- `Columnar <https://docs.columnar.tech/drivers/>`__: ...TODO

Please file an `issue <https://github.com/apache/arrow-adbc/issues>`__ to add new entries to the above list.

Installing Third-Party Drivers
------------------------------

Many third-party drivers are built as shared libraries according to the ADBC Driver Manifests specification.
Both official and third-party drivers are distributed by `Columnar <https://columnar.tech>`__ and can be installed with their command line tool, `dbc <https://columnar.tech/dbc>`__:

.. button-link:: https://columnar.tech/dbc
  :color: primary

  Get dbc :octicon:`cross-reference`

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
