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

At this time, separate packages for C/C++-based drivers are not available.

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

Install the appropriate driver package from PyPI, for example:

- ``pip install adbc_driver_postgresql``
- ``pip install adbc_driver_sqlite``
