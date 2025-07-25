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

.. py:currentmodule:: adbc_driver_manager

==============
Driver Manager
==============

The driver manager is a library that provides bindings to the ADBC C
API.  It delegates to dynamically-loaded drivers.  This allows
applications to use multiple drivers simultaneously, and decouple
themselves from the specific driver.

The Python driver manager provides both low-level bindings that are
essentially the same as the C API.  If PyArrow is installed, it also
provides high-level bindings that implement the DBAPI_ (PEP 249)
standard.

.. _DBAPI: https://peps.python.org/pep-0249/

Installation
============

.. code-block:: shell

   pip install adbc_driver_manager

Usage
=====

.. warning:: This API is for low level usage only.  **You almost certainly
             should not use this**, instead use the entrypoints provided by
             driver packages, for example:

             - :func:`adbc_driver_sqlite.dbapi.connect`
             - :func:`adbc_driver_sqlite.connect`

The Python bindings for each driver abstract the steps here for you behind a
convenient ``connect`` function.  For example, prefer
:func:`adbc_driver_sqlite.connect` or :func:`adbc_driver_postgresql.connect`
to manually constructing the connection as demonstrated here.

To manually create a connection: first, create a :py:class:`AdbcDatabase`,
passing ``driver`` and (optionally) ``entrypoint``.  ``driver`` must be the
name of a library to load, or the path to a library to load.  ``entrypoint``,
if provided, should be the name of the symbol that serves as the ADBC
entrypoint (see :c:type:`AdbcDriverInitFunc`).  Then, create a
:py:class:`AdbcConnection`.

.. code-block:: python

   import adbc_driver_manager

   # You must build/locate the driver yourself
   with adbc_driver_manager.AdbcDatabase(driver="PATH/TO/libadbc_driver_sqlite.so") as db:
       with adbc_driver_manager.AdbcConnection(db) as conn:
           pass

API Reference
=============

See the API reference: https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc.
