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

First create a :py:class:`AdbcDatabase`, passing ``driver`` and
(optionally) ``entrypoint``.  ``driver`` must be the name of a library
to load, or the path to a library to load.  ``entrypoint``, if
provided, should be the name of the symbol that serves as the ADBC
entrypoint (see :cpp:type:`AdbcDriverInitFunc`).  Then, create a
:py:class:`AdbcConnection`.

.. code-block:: python

   import adbc_driver_manager
   with adbc_driver_manager.AdbcDatabase(driver="adbc_driver_sqlite") as db:
       with adbc_driver_manager.AdbcConnection(db) as conn:
           pass

The Python bindings for each driver abstract these steps for you
behind a convenient ``connect`` function.

API Reference
=============

See the API reference: :doc:`./api/adbc_driver_manager`.
