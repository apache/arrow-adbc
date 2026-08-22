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

Here we'll briefly tour basic features of ADBC with Rust using the SQLite driver.

Installation
============

Add a dependency on ``adbc_core`` and ``adbc_driver_manager``:

.. code-block:: shell

   cargo add adbc_core adbc_driver_manager

.. note:: If you get a compiler error (E0308, mismatched types) and a note about
          multiple versions of the arrow crates in the dependency graph when you
          run ``cargo build``, you can downgrade crate versions so you only have
          one version of the arrow crates in your workspace:

          .. code-block:: shell

             cargo update -p arrow-array@57.0.0 -p arrow-schema@57.0.0 --precise 56.2.0

          Note the exact versions in the command above may need to be changed.
          Use ``cargo tree`` to find the versions affecting your workspace. See
          the `Version Incompatibility Hazards <https://doc.rust-lang.org/cargo/reference/resolver.html#version-incompatibility-hazards>`__
          in the Cargo documentation for more information.

Installing Drivers
------------------

See :ref:`driver-table-install` for instructions on installing ADBC drivers for
the database you want to connect to. For the example below, you could install
`dbc <https://docs.columnar.tech/dbc>`__ and then install the SQLite driver with:

.. code-block:: shell

   dbc install sqlite

Loading a Driver
================

Use ``ManagedDriver::load_from_name`` to locate and load a driver by name. The driver manager
searches for a driver manifest (e.g. ``sqlite.toml``) in the directories controlled by
``load_flags``. See :doc:`/format/driver_manifests` for details on manifest search paths.

.. code-block:: rust

   use adbc_core::options::AdbcVersion;
   use adbc_core::{Connection, Database, Driver, Statement, LOAD_FLAG_DEFAULT};
   use adbc_driver_manager::ManagedDriver;

   let mut driver = ManagedDriver::load_from_name(
       "sqlite",
       None,
       AdbcVersion::default(),
       LOAD_FLAG_DEFAULT,
       None,
   )
   .expect("Failed to load driver");
   let db = driver.new_database().expect("Failed to create database handle");
   let mut conn = db.new_connection().expect("Failed to create connection");

Running Queries
===============

To run queries, create a statement and set a query:

.. code-block:: rust

   let mut stmt = conn.new_statement().expect("Failed to create statement");
   stmt.set_sql_query("SELECT 1").expect("Failed to set SQL query");

Execute the query to get an Arrow ``RecordBatchReader``:

.. code-block:: rust

   let reader = stmt.execute().expect("Failed to execute statement");
   for batch in reader {
       let batch = batch.expect("Failed to read batch");
       println!("{:?}", batch);
   }

Next Steps
==========

- Check out the :doc:`Rust API documentation <index>` for more details
- See the :doc:`drivers </driver/index>` to see which databases are supported
- Explore more examples in the `adbc-quickstarts repository <https://github.com/columnar-tech/adbc-quickstarts/tree/main/rust>`_
- Explore the `Rust source code <https://github.com/apache/arrow-adbc/tree/main/rust>`_ for additional examples
