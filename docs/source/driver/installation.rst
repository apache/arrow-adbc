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

============
Installation
============

.. note::

   See individual driver pages in the sidebar for specific installation instructions.

C/C++
=====

Install the appropriate driver package.  These are currently only available from conda-forge_:

- ``mamba install libadbc-driver-flightsql``
- ``mamba install libadbc-driver-postgresql``
- ``mamba install libadbc-driver-sqlite``

Then they can be used via CMake, e.g.:

.. code-block:: cmake

   find_package(AdbcDriverPostgreSQL)

   # ...

   target_link_libraries(myapp PRIVATE AdbcDriverPostgreSQL::adbc_driver_postgresql_shared)

.. _conda-forge: https://conda-forge.org/

Go
==

Add a dependency on the driver package, for example:

- ``go get -u github.com/apache/arrow-adbc/go/adbc@latest``
- ``go get -u github.com/apache/arrow-adbc/go/adbc/driver/flightsql@latest``

Java
====

Add a dependency on the driver package, for example:

- ``org.apache.arrow.adbc:adbc-driver-flight-sql``
- ``org.apache.arrow.adbc:adbc-driver-jdbc``

Python
======

Install the appropriate driver package.

For example, from PyPI:

- ``pip install adbc-driver-flightsql``
- ``pip install adbc-driver-postgresql``
- ``pip install adbc-driver-sqlite``

From conda-forge_:

- ``mamba install adbc-driver-flightsql``
- ``mamba install adbc-driver-postgresql``
- ``mamba install adbc-driver-sqlite``
