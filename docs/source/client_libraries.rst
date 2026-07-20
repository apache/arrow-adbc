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

================
Client Libraries
================

ADBC client libraries let you connect to databases and execute queries from your language of choice.
Because most ADBC :doc:`drivers <driver/index>` expose the same :doc:`standard C ABI <format/specification>`, a single client library can generally use drivers written in any language through the same client API.

.. note::

   On this page we use the terms **client library** and **driver manager**
   interchangeably. More precisely, the driver manager is the part of a client
   library that dynamically loads ADBC driver shared libraries and forwards your
   calls to them; the client library wraps that machinery in an API that feels
   idiomatic in your language.

   The package names are, unfortunately, not consistent about this. Some are
   named for the driver manager (Python's ``adbc-driver-manager``, R's
   ``adbcdrivermanager``), while others are named for ADBC itself (C#'s
   ``Apache.Arrow.Adbc.Client``, Ruby's ``red-adbc``). They all give you the
   same thing: a way to load drivers and talk to databases from your language.

Packages
========

The client library package for each language:

.. list-table::
   :header-rows: 1
   :widths: 15 15 30 40

   * - Language
     - Package Manager
     - Package
     - Documentation
   * - Python
     - pip / conda
     - ``adbc-driver-manager``
     - :doc:`Python Quickstart <python/quickstart>`
   * - C/C++
     - various
     - See quickstart
     - :doc:`C/C++ Quickstart <cpp/quickstart>`
   * - C#
     - dotnet
     - ``Apache.Arrow.Adbc.Client``
     - :doc:`C#/.NET <csharp/index>`
   * - Go
     - go
     - ``github.com/apache/arrow-adbc/go/adbc``
     - `Go Documentation ↗ <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`__
   * - Java
     - Maven
     - ``org.apache.arrow.adbc``: ``adbc-core``, ``adbc-driver-manager``, ``adbc-driver-jni``
     - :doc:`Java Quickstart <java/quickstart>`
   * - JavaScript / TypeScript
     - npm
     - ``@apache-arrow/adbc-driver-manager``
     - :doc:`JavaScript Quickstart <javascript/quickstart>`
   * - R
     - CRAN / conda
     - ``adbcdrivermanager``
     - :doc:`R <r/index>`
   * - Ruby
     - bundler
     - ``red-adbc``
     - :doc:`Ruby <ruby/index>`
   * - Rust
     - cargo
     - ``adbc_core``, ``adbc_driver_manager``
     - :doc:`Rust Quickstart <rust/quickstart>`

Using a Client Library
======================

Client libraries provide a language-native interface on top of the ADBC API.
They handle loading drivers, managing connections, and converting results into idiomatic types for each language.
For example, Python exposes a `DBAPI 2.0 (PEP 249) <https://peps.python.org/pep-0249/>`__-style interface, Go uses ``database/sql``, and R uses DBI.

A client library on its own can't talk to a database—it also needs a
:doc:`driver <driver/index>` for the database you want to connect to.
The client library loads the driver at runtime, so you install the client
library once and then add a driver for each database you use.
See :doc:`Drivers <driver/index>` for the full list of available drivers and how to install them.

You may not need to use a client library directly at all: many higher-level
tools, such as pandas and Polars, integrate with ADBC and use it under the hood.
See :doc:`Tools & Integrations <integrations>` for more.

More Information
================

- `ADBC Quickstarts <https://github.com/columnar-tech/adbc-quickstarts>`__ — simple, runnable examples for getting started in each of these languages, across a range of databases
