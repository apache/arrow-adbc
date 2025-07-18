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
JDBC Adapter
============

.. adbc_driver_status:: ../../../java/driver/jdbc/README.md

The JDBC Adapter provides access to any database with a JDBC driver.

Installation
============

.. adbc_driver_installation:: ../../../java/driver/jdbc/README.md

Usage
=====

To connect to a database, supply the JDBC URI as the "uri" parameter,
or an instance of a ``javax.sql.DataSource`` as the
"adbc.jdbc.datasource" parameter.

.. tab-set::

   .. tab-item:: Java
      :sync: java

      .. code-block:: java

         final Map<String, Object> parameters = new HashMap<>();
         AdbcDriver.PARAM_URI.set(parameters, "jdbc:postgresql://localhost:5432/postgres");
         AdbcDatabase db = new JdbcDriver(allocator).open(parameters);

Supported Features
==================

The JDBC Adapter generally supports features defined in the ADBC
API specification 1.0.0.
