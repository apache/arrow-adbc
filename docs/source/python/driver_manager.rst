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

The ``adbc_driver_manager`` package provides a :term:`driver manager` interface
for Python. The package provides two APIs:

1. Low-level bindings that are essentially the same as the :doc:`C API <../format/specification>`.
2. If PyArrow is installed, a DBAPI_ (PEP 249) compliant interface.

.. _DBAPI: https://peps.python.org/pep-0249/

Installation
============

.. code-block:: shell

   pip install adbc_driver_manager

Usage
=====

The driver manager is different from the individual, driver-specific packages
such as ``adbc_driver_postgresql``.

With the driver-specific packages, you connect to the target database with the
``connect`` method provided by the package you're using.  For example,
:func:`adbc_driver_postgresql.connect` or :func:`adbc_driver_sqlite.connect`.

With the driver manager package, you use a single package and API regardless of
the database you're connecting to.

Low-Level API
-------------

First, create an :py:class:`AdbcDatabase`, passing ``driver`` and (optionally)
``entrypoint``. Then, create an :py:class:`AdbcConnection`.

.. note:: See :doc:`../format/driver_manifests` for more information on what to pass as the ``driver`` argument and how the driver manager finds and loads drivers.

.. code-block:: python

   import adbc_driver_manager

   # Note: You must install the driver shared library
   (``.so``/``.dylib``/``.dll`` file) separately
   with adbc_driver_manager.AdbcDatabase(driver="PATH/TO/libadbc_driver_sqlite.so") as db:
       with adbc_driver_manager.AdbcConnection(db) as conn:
           pass

Connecting to a second database could be done in the same session using the same
code just with a different ``driver`` argument.

DBAPI API
---------

Use the DBAPI_ API by calling the ``dbapi.connect`` method, passing ``driver``
and (optionally) ``entrypoint``. These arguments work the same as with the
low-level API.

.. code-block:: python

   from adbc_driver_manager import dbapi

   # Note: You must install the driver shared library
   (``.so``/``.dylib``/``.dll`` file) separately
   with dbapi.connect(driver="PATH/TO/libadbc_driver_sqlite.so") as conn:
      pass


API Reference
=============

See the API reference: :doc:`./api/adbc_driver_manager`.
