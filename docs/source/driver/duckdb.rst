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

      You must have ``libduckdb.so`` on your ``LD_LIBRARY_PATH``, or
      in the same directory as the executable when you run this.  This
      requires CGO and loads the DuckDB driver.

      .. code-block:: go

         import (
            "context"

            "github.com/apache/arrow-adbc/go/adbc"
            "github.com/apache/arrow-adbc/go/adbc/drivermgr"
         )

         func main() {
            var drv drivermgr.Driver
            db, err := drv.NewDatabase(map[string]string{
               "driver": "libduckdb.so",
               "entrypoint": "duckdb_adbc_init",
            })
            if err != nil {
               // handle error
            }
            defer db.Close()

            cnxn, err := db.Open(context.Background())
            if err != nil {
               // handle error
            }
            defer cnxn.Close()
         }

   .. tab-item:: Python
      :sync: python

      You must have DuckDB 0.9.1 or higher.

      See the `DuckDB Python documentation`_.

   .. tab-item:: R
      :sync: r

      You must have DuckDB 0.9.1 or higher.

      .. code-block:: r

         # install.packages("duckdb")
         library(adbcdrivermanager)
         db <- adbc_database_init(duckdb::duckdb_adbc(), ...)


.. _DuckDB C++ documentation: https://duckdb.org/docs/api/adbc.html#c
.. _DuckDB Python documentation: https://duckdb.org/docs/api/adbc.html#python

Supported Features
==================

ADBC support in DuckDB is still in progress.  See `duckdb/duckdb#7141
<https://github.com/duckdb/duckdb/issues/7141>`_ for details.
