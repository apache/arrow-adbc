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
Driver Manager
==============

The driver manager is a library that provides bindings to the ADBC Java API. It delegates to dynamically-loaded drivers. This allows applications to use multiple drivers simultaneously and decouple themselves from the specific driver.

The Java driver manager provides both low-level bindings that are essentially the same as the C API and high-level bindings that implement the JDBC standard.

Installation
============

To include the ADBC Driver Manager in your Maven project, add the following dependency:

.. code-block:: xml

   <dependency>
       <groupId>com.adbc</groupId>
       <artifactId>adbc-driver-manager</artifactId>
       <version>1.0.0</version>
   </dependency>

Usage
=====

First, load the driver:

.. code-block:: java

   import org.apache.arrow.adbc.core.AdbcDatabase;
   import org.apache.arrow.adbc.core.AdbcDriver;
   import org.apache.arrow.adbc.drivermanager.AdbcDriverManager;

Then, create a connection:

.. code-block:: java

   Connection conn = DriverManager.getConnection("jdbc:adbc:sqlite:sample.db");

API Reference
=============

See the API reference: :doc:`./api/adbc_driver_manager`.
