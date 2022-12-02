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

To build the libraries from source, see :doc:`../contributing`.

.. _cpp-install-driver-manager:

ADBC Driver Manager
===================

.. _cpp-install-flight-sql:

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. note:: Under construction

   .. tab-item:: Python (pip)
      :sync: python

      .. note:: Under construction

   .. tab-item:: Python (conda-forge)
      :sync: python-conda-forge

      .. note:: Under construction

Flight SQL Driver
=================

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      Link to the Arrow C++ libraries, in particlar the Flight SQL
      libraries.  See :doc:`arrow:cpp/build_system`.

   .. tab-item:: Python (pip)
      :sync: python

      1. Install PyArrow 11.0.0 or greater.  See
         :doc:`arrow:python/install`.
      2. :ref:`Install the Python ADBC Driver Manager
         <cpp-install-driver-manager>`.
      3. Import ``pyarrow.flight_sql`` for a `DBAPI 2.0`_-compatible
         interface.

   .. tab-item:: Python (conda-forge)
      :sync: python-conda-forge

      1. Install PyArrow 11.0.0 or greater.  See
         :doc:`arrow:python/install`.
      2. :ref:`Install the Python ADBC Driver Manager
         <cpp-install-driver-manager>`.
      3. Import ``pyarrow.flight_sql`` for a `DBAPI 2.0`_-compatible
         interface.

.. _cpp-install-libpq:

libpq-based Driver
==================

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. note:: Under construction

   .. tab-item:: Python (pip)
      :sync: python

      .. note:: Under construction

   .. tab-item:: Python (conda-forge)
      :sync: python-conda-forge

      .. note:: Under construction

.. _cpp-install-sqlite:

SQLite Driver
=============

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. note:: Under construction

   .. tab-item:: Python (pip)
      :sync: python

      .. note:: Under construction

   .. tab-item:: Python (conda-forge)
      :sync: python-conda-forge

      .. note:: Under construction

.. _DBAPI 2.0: https://peps.python.org/pep-0249/
