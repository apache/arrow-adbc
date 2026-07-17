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

Here we'll briefly tour basic features of ADBC with C# using the PostgreSQL driver.

Installation
============

.. code-block:: shell

   dotnet add package Apache.Arrow.Adbc.Client

Basic Example
=============

This example demonstrates connecting to a database, executing a query, and reading results using the ADO.NET-style API.

.. code-block:: csharp

   using Apache.Arrow.Adbc.Client;
   using System;
   using System.Data.Common;

   class Program
   {
       static void Main()
       {
           // Create a connection string
           string connectionString = "Driver=postgresql;uri=postgresql://user:password@localhost:5432/database";

           using var connection = new AdbcConnection(connectionString);
           connection.Open();

           // Execute a query
           using var command = connection.CreateCommand();
           command.CommandText = "SELECT * FROM my_table";

           using var reader = command.ExecuteReader();
           while (reader.Read())
           {
               // Access data using column index or name
               Console.WriteLine($"Value: {reader[0]}");
           }
       }
   }

Working with Arrow Data
=======================

ADBC natively works with Apache Arrow data structures:

.. code-block:: csharp

   using Apache.Arrow;
   using Apache.Arrow.Adbc.Client;

   string connectionString = "Driver=postgresql;uri=postgresql://user:password@localhost:5432/database";

   using var connection = new AdbcConnection(connectionString);
   connection.Open();

   using var command = connection.CreateCommand();
   command.CommandText = "SELECT * FROM my_table";

   // Get results as Arrow data
   using var reader = command.ExecuteReader();

   // Process Arrow record batches
   while (reader.Read())
   {
       // Reader provides access to underlying Arrow data
       for (int i = 0; i < reader.FieldCount; i++)
       {
           Console.WriteLine($"{reader.GetName(i)}: {reader.GetValue(i)}");
       }
   }

Executing Non-Query Commands
=============================

For INSERT, UPDATE, DELETE, or DDL statements:

.. code-block:: csharp

   using var connection = new AdbcConnection(connectionString);
   connection.Open();

   using var command = connection.CreateCommand();
   command.CommandText = "INSERT INTO my_table (id, name) VALUES (1, 'test')";

   int rowsAffected = command.ExecuteNonQuery();
   Console.WriteLine($"Rows affected: {rowsAffected}");

Parameterized Queries
=====================

Use parameterized queries for safe SQL execution:

.. code-block:: csharp

   using var connection = new AdbcConnection(connectionString);
   connection.Open();

   using var command = connection.CreateCommand();
   command.CommandText = "SELECT * FROM my_table WHERE id = @id";

   var parameter = command.CreateParameter();
   parameter.ParameterName = "@id";
   parameter.Value = 42;
   command.Parameters.Add(parameter);

   using var reader = command.ExecuteReader();
   while (reader.Read())
   {
       Console.WriteLine(reader[0]);
   }

Installing Drivers
==================

See :ref:`driver-table-install` for instructions on installing ADBC drivers for
the database you want to connect to. For the example below, you could install
`dbc <https://docs.columnar.tech/dbc>`__ and run ``dbc install postgresql``.

Native Drivers
--------------

C# ADBC also includes native drivers for some databases:

.. code-block:: shell

   # BigQuery native driver
   dotnet add package Apache.Arrow.Adbc.Drivers.BigQuery

See the :doc:`driver documentation </driver/index>` for more details.

Next Steps
==========

- Check out the :doc:`C#/.NET API documentation <index>` for more details
- See the :doc:`drivers </driver/index>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/csharp>`_
- Explore the `C# source code <https://github.com/apache/arrow-adbc/tree/main/csharp>`_ for additional examples
