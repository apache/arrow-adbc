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

:orphan:

==============
DuckDB Support
==============

.. adbc_driver_status:: ../../../c/integration/duckdb/README.md

`DuckDB`_ provides ADBC support since `version 0.8.0
<https://duckdb.org/2023/05/17/announcing-duckdb-080.html>`_.

.. note:: DuckDB is not part of the Apache Arrow project and is
          developed separately.

.. _DuckDB: https://duckdb.org/

Installation
============

See the `DuckDB documentation
<https://duckdb.org/docs/installation/>`_.

Usage
=====

ADBC support in DuckDB requires the driver manager.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      See the `DuckDB C++ documentation`_.

   .. tab-item:: Go
      :sync: go

      See the `DuckDB Go documentation`_.

   .. tab-item:: Python
      :sync: python

      See the `DuckDB Python documentation`_.

   .. tab-item:: R
      :sync: r

      You must have DuckDB 0.9.1 or higher.

      .. code-block:: r

         # install.packages("duckdb")
         library(adbcdrivermanager)
         db <- adbc_database_init(duckdb::duckdb_adbc(), ...)


.. _DuckDB C++ documentation: https://duckdb.org/docs/current/clients/adbc#c
.. _DuckDB Python documentation: https://duckdb.org/docs/current/clients/adbc#python
.. _DuckDB Go documentation: https://duckdb.org/docs/current/clients/adbc#go

Supported Features
==================

DuckDB version 0.8.0 to 1.4.x supports the ADBC 1.0.0 spec,
and version 1.5.0 and later support the ADBC 1.1.0 spec.
