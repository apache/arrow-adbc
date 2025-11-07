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
Driver Example
==============

.. recipe:: recipe_driver/driver_example.cc

Low-level testing
=================

.. recipe:: recipe_driver/driver_example_test.cc

High-level testing
==================

.. recipe:: recipe_driver/driver_example.py

High-level tests can also be written in R using the ``adbcdrivermanager``
package.

.. code-block:: r

   library(adbcdrivermanager)

   drv <- adbc_driver("build/libdriver_example.dylib")
   db <- adbc_database_init(drv, uri = paste0("file://", getwd()))
   con <- adbc_connection_init(db)

   data.frame(col = 1:3) |> write_adbc(con, "example.arrows")
   con |> read_adbc("SELECT * FROM example.arrows") |> as.data.frame()
   unlink("example.arrows")

Driver Manifests
================

.. recipe:: recipe_driver/driver_example_manifest.py

Driver manifests can provide an easier way to install and manage ADBC drivers,
via TOML files that describe some metadata along with the path to the driver
shared library. The driver manager can read these manifests to locate and load
drivers dynamically.
