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

============================
``Java ADBC Driver Manager``
============================

Java ADBC Wrapper for JDBC
==========================

The Java ADBC Driver Manager provides a means to manage ADBC drivers and facilitate connections to databases using the ADBC API. This particular implementation wraps around the JDBC API.

Constants
---------

.. data:: org.apache.arrow.adbc.driver.jdbc.JdbcDriver.PARAM_DATASOURCE

   A parameter for creating an ``AdbcDatabase`` from a ``DataSource``.

.. data:: org.apache.arrow.adbc.driver.jdbc.JdbcDriver.PARAM_JDBC_QUIRKS

   A parameter for specifying backend-specific configuration.

.. data:: org.apache.arrow.adbc.driver.jdbc.JdbcDriver.PARAM_URI

   A parameter for specifying a URI to connect to, aligning with the C/Go implementations.

Classes
-------

.. class:: org.apache.arrow.adbc.driver.jdbc.JdbcDriver

   An ADBC driver implementation that wraps around the JDBC API.

   .. method:: open(Map<String, Object> parameters)

      Opens a new database connection using the specified parameters.

.. class:: org.apache.arrow.adbc.driver.jdbc.JdbcDataSourceDatabase

   Represents an ADBC database backed by a JDBC ``DataSource``.

Utilities
---------

The ``JdbcDriver`` class provides utility methods to fetch and validate parameters from the provided options map.

.. method:: org.apache.arrow.adbc.driver.jdbc.JdbcDriver.getParam(Class<T> klass, Map<String, Object> parameters, String... choices)

   Retrieves a parameter from the provided map, validating its type and ensuring no duplicates.

Usage
=====

The ``JdbcDriver`` class is registered with the ``AdbcDriverManager`` upon class loading. To utilize this driver:

1. Ensure the necessary dependencies are in place.
2. Create a ``Map<String, Object>`` containing the connection parameters.
3. Use the ``AdbcDriverManager`` to obtain an instance of the ``JdbcDriver``.
4. Open a new database connection using the driver's ``open`` method.

Exceptions
==========

Any errors during the driver operations throw the ``AdbcException``. This exception provides detailed messages indicating the nature of the problem.
