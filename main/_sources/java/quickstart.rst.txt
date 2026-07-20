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

==========
Quickstart
==========

Here we'll briefly tour basic features of ADBC with Java :doc:`JNI driver manager <./jni>` and the PostgreSQL driver.

Installation
============

To include ADBC in your Maven project, add the following dependency:

.. code-block:: xml

   <dependency>
       <groupId>org.apache.arrow.adbc</groupId>
       <artifactId>adbc-driver-jni</artifactId>
       <version>${adbc.version}</version>
   </dependency>

Installing Drivers
------------------

See :ref:`driver-table-install` for instructions on installing ADBC drivers for
the database you want to connect to. For the example below, you could install
`dbc <https://docs.columnar.tech/dbc>`__ and then install the PostgreSQL driver with:

.. code-block:: shell

   dbc install postgresql

Imports
-------

For the examples in this section, the following imports are required:

.. code-block:: java

   import java.util.HashMap;
   import java.util.Map;
   import org.apache.arrow.adbc.core.AdbcConnection;
   import org.apache.arrow.adbc.core.AdbcDatabase;
   import org.apache.arrow.adbc.core.AdbcDriver;
   import org.apache.arrow.adbc.core.AdbcException;
   import org.apache.arrow.adbc.core.AdbcStatement;
   import org.apache.arrow.adbc.driver.jni.JniDriver;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;

Basic Usage
===========

.. code-block:: java

   Map<String, Object> parameters = new HashMap<>();
   JniDriver.PARAM_DRIVER.set(parameters, "postgresql");
   AdbcDriver.PARAM_URI.set(parameters, "postgresql://localhost:5432/postgres");
   try (
       BufferAllocator allocator = new RootAllocator();
       AdbcDatabase db = new JniDriver(allocator).open(parameters);
       AdbcConnection adbcConnection = db.connect();
       AdbcStatement stmt = adbcConnection.createStatement();
   ) {
       stmt.setSqlQuery("select * from foo");
       try (AdbcStatement.QueryResult queryResult = stmt.executeQuery()) {
           while (queryResult.getReader().loadNextBatch()) {
               // process batch
           }
       }
   } catch (AdbcException e) {
       // throw
   }

In application code, the connection must be closed after usage or memory may leak.
It is recommended to wrap the connection in a try-with-resources block for automatic
resource management. In this example, we are connecting to a PostgreSQL database,
specifically the default database "postgres".

Note that creating a statement is also wrapped in the try-with-resources block.
Assuming we have a table "foo" in the database, an example for setting and executing the
query is also provided.

Next Steps
==========

- Check out the :doc:`Java API documentation <index>` for more details
- See the :doc:`drivers </driver/index>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/java>`_
- Explore the `Java source code <https://github.com/apache/arrow-adbc/tree/main/java>`_ for additional examples
