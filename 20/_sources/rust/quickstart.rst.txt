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

Here we'll briefly tour using ADBC with the `DataFusion`_ driver.

.. _DataFusion: https://datafusion.apache.org/

Installation
============

Add a dependency on ``adbc_core`` and ``adbc_datafusion``:

.. code-block:: shell

   cargo add adbc_core adbc_datafusion

Loading DataFusion
==================

Create a driver instance, then a database handle, and then finally a
connection.  (This is a bit redundant for something like DataFusion, but the
intent is that the database handle can hold shared state that multiple
connections can share.)

.. code-block:: rust

   // These traits must be in scope
   use adbc_core::{Connection, Database, Driver, Statement};

   let mut driver = adbc_datafusion::DataFusionDriver {};
   let db = driver.new_database().expect("Failed to create database handle");
   let mut conn = db.new_connection().expect("Failed to create connection");

Running Queries
===============

To run queries, we can create a statement and set a query:

.. code-block:: rust

   let mut stmt = conn.new_statement().expect("Failed to create statement");
   stmt.set_sql_query("SELECT 1").expect("Failed to set SQL query");

We can then execute the query to get an Arrow ``RecordBatchReader``:

.. code-block:: rust

   let reader = stmt.execute().expect("Failed to execute statement");
   for batch in reader {
       let batch = batch.expect("Failed to read batch");
       println!("{:?}", batch);
   }
