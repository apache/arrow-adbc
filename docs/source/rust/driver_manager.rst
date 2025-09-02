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

This document describes the installation and usage of the Rust :term:`driver
manager`. The Rust :term:`driver manager` is currently part of the ``adbc_core``
package, though we plan to split it into its own package for to keep the
``adbc_core`` package smaller for users who don't want or need FFI.

Installation
============

.. code-block:: shell

   cargo add adbc_core --features driver_manager

Usage
=====

.. code-block:: rust

    use adbc_core::{Connection, Database, Driver, Statement};
    use adbc_core::options::{AdbcVersion, OptionDatabase};
    use adbc_core::driver_manager::ManagedDriver;

    // You must build/locate the driver yourself
    let mut driver = ManagedDriver::load_dynamic_from_filename(
        "/PATH/TO/libadbc_driver_sqlite.so",
        None,
        AdbcVersion::default(),
    ).expect("Failed to load driver");
    let db = driver.new_database().expect("Failed to create database handle");
    let mut conn = db.new_connection().expect("Failed to create connection");

API Reference
=============

See the API reference: `Module driver_manager <https://docs.rs/adbc_core/latest/adbc_core/driver_manager/>`_.
