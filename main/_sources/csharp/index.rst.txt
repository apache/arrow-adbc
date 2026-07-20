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

=======
C#/.NET
=======

The ADBC C#/.NET libraries support three different ways of using ADBC:

- Native C# drivers for databases like Google BigQuery
- Bindings packages that wrap individual drivers such as the Snowflake Go driver
- A driver manager for loading ADBC drivers written in other languages at runtime

Here we'll briefly tour basic features using the driver manager and the PostgreSQL driver.

Installation
============

.. code-block:: shell

   dotnet add package Apache.Arrow.Adbc

Installing Drivers
------------------

See :ref:`driver-table-install` for instructions on installing ADBC drivers for
the database you want to connect to. For the example below, you could install
`dbc <https://docs.columnar.tech/dbc>`__ and then install the PostgreSQL driver with:

.. code-block:: shell

   dbc install postgresql

Usings
------

.. code-block:: csharp

   using System.Collections.Generic;
   using Apache.Arrow;
   using Apache.Arrow.Adbc;
   using Apache.Arrow.Adbc.DriverManager;
   using Apache.Arrow.Ipc;

Basic Usage
===========

.. code-block:: csharp

   using AdbcDriver driver = AdbcDriverManager.FindLoadDriver(
       "postgresql",
       loadOptions: AdbcLoadFlags.Default);

   using AdbcDatabase db = driver.Open(new Dictionary<string, string>
   {
       ["uri"] = "postgresql://localhost:5432/postgres",
   });

   using AdbcConnection conn = db.Connect(null);
   using AdbcStatement stmt = conn.CreateStatement();

   stmt.SqlQuery = "SELECT * FROM foo";

   QueryResult result = stmt.ExecuteQuery();
   using IArrowArrayStream stream = result.Stream!;

   while (true)
   {
       using Apache.Arrow.RecordBatch batch = await stream.ReadNextRecordBatchAsync();
       if (batch == null) break;
       // process batch
   }

Next Steps
==========

- Explore more C# examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/csharp>`_
