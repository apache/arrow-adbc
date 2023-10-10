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

Here we'll briefly tour basic features of ADBC with the PostgreSQL driver for Java.

Installation
============

To include ADBC in your Maven project, add the following dependency:

.. code-block:: xml

   <dependency>
       <groupId>org.apache.arrow.adbc</groupId>
       <artifactId>adbc-driver-jdbc</artifactId>
       <version>0.7.0</version>
   </dependency>

For the examples in this section, the following imports are required:

.. code-block:: java

   import org.apache.arrow.adbc.core.AdbcConnection;
   import org.apache.arrow.adbc.core.AdbcDatabase;
   import org.apache.arrow.adbc.core.AdbcDriver;
   import org.apache.arrow.adbc.core.AdbcException;
   import org.apache.arrow.adbc.core.AdbcStatement;

JDBC-style API
==============

ADBC provides a high-level API in the style of the JDBC standard.

Creating a Connection
---------------------

.. code-block:: java

   final Map<String, Object> parameters = new HashMap<>();
   parameters.put(AdbcDriver.PARAM_URL, "jdbc:postgresql://localhost:5432/postgres");
   try (
       BufferAllocator allocator = new RootAllocator();
       AdbcDatabase db = new JdbcDriver(allocator).open(parameters);
       AdbcConnection adbcConnection = db.connect();

   ) {
       // run queries, etc.
   }  catch (AdbcException e) {
       // throw
   }

In application code, the connection must be closed after usage or memory may leak.
It is recommended to wrap the connection in a try-resource block for automatic
resource management.  In this example, we are connecting to a PostgreSQL database,
specifically the default database "postgres".

Creating a Statement
--------------------

.. code-block:: java

   AdbcStatement stmt = adbcConnection.createStatement();

Executing a Query
-----------------

.. code-block:: java

   stmt.setSqlQuery("select * from foo");
   AdbcStatement.QueryResult queryResult = stmt.executeQuery();
   while (queryResult.getReader().loadNextBatch()) {
       // process batch
   }
