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

.. note:: **"Client library" and "driver manager"**

   On this page we use the terms **client library** and **driver manager**
   interchangeably. More precisely, the *driver manager* is the part of a client
   library that dynamically loads ADBC driver shared libraries and forwards your
   calls to them; the client library wraps that machinery in an API that feels
   idiomatic in your language.

   The package names are, unfortunately, not consistent about this. Some are
   named for the driver manager (Python's ``adbc-driver-manager``, R's
   ``adbcdrivermanager``), while others are named for ADBC itself (C#'s
   ``Apache.Arrow.Adbc.Client``, Ruby's ``red-adbc``). They all give you the
   same thing: a way to load drivers and talk to databases from your language.
   We apologize for the confusion.

Quick Start
===========

Get started with ADBC in your language of choice:

.. tab-set::

   .. tab-item:: Python

      .. code-block:: bash

         pip install adbc-driver-manager

      See the :doc:`Python Quickstart <python/quickstart>` for usage instructions.

   .. tab-item:: C/C++

      .. code-block:: bash

         conda install libadbc-driver-manager

      Or vendor ``adbc.h`` directly. See the :doc:`C/C++ Quickstart <cpp/quickstart>` for installation and usage instructions.

   .. tab-item:: C#

      .. code-block:: bash

         dotnet add package Apache.Arrow.Adbc.Client

      Or install ``Apache.Arrow.Adbc`` for the lower-level API. See the :doc:`C# Quickstart <csharp/quickstart>` for usage instructions.

   .. tab-item:: Go

      .. code-block:: bash

         go get github.com/apache/arrow-adbc/go/adbc

      See the `Go Documentation <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`_ for usage instructions.

   .. tab-item:: Java

      .. code-block:: xml

         <dependency>
           <groupId>org.apache.arrow.adbc</groupId>
           <artifactId>adbc-core</artifactId>
         </dependency>
         <dependency>
           <groupId>org.apache.arrow.adbc</groupId>
           <artifactId>adbc-driver-manager</artifactId>
         </dependency>
         <dependency>
           <groupId>org.apache.arrow.adbc</groupId>
           <artifactId>adbc-driver-jni</artifactId>
         </dependency>

      See the :doc:`Java Quickstart <java/quickstart>` for usage instructions.

   .. tab-item:: JS/TS

      .. code-block:: bash

         npm install @apache-arrow/adbc-driver-manager

      See the :doc:`JavaScript Quickstart <javascript/quickstart>` for usage instructions.

   .. tab-item:: R

      .. code-block:: r

         install.packages("adbcdrivermanager")

      See the :doc:`R Quickstart <r/quickstart>` for usage instructions.

   .. tab-item:: Ruby

      .. code-block:: bash

         bundle add red-adbc

      See the :doc:`Ruby Quickstart <ruby/quickstart>` for usage instructions.

   .. tab-item:: Rust

      .. code-block:: bash

         cargo add adbc_core adbc_driver_manager

      See the :doc:`Rust Quickstart <rust/quickstart>` for usage instructions.

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
     - :doc:`C# Quickstart <csharp/quickstart>`
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
     - :doc:`R Quickstart <r/quickstart>`
   * - Ruby
     - bundler
     - ``red-adbc``
     - :doc:`Ruby Quickstart <ruby/quickstart>`
   * - Rust
     - cargo
     - ``adbc_core``, ``adbc_driver_manager``
     - :doc:`Rust Quickstart <rust/quickstart>`

Using a Client Library
======================

Client libraries provide a language-native interface on top of the ADBC API.
They handle loading drivers, managing connections, and converting results into idiomatic types for each language.
For example, Python exposes a `DBAPI 2.0 (PEP 249) <https://peps.python.org/pep-0249/>`__-style interface, Go uses ``database/sql``, and R uses DBI.

To use a client library you need both the library itself and a :doc:`driver <driver/index>` for the database you want to connect to.
The :doc:`ADBC Driver Manager <format/how_manager>` handles loading the driver at run-time, so in most languages you install a separate package per driver (e.g. ``adbc_driver_sqlite`` in Python) alongside the client library.

More information
================

- `ADBC Quickstarts <https://github.com/columnar-tech/adbc-quickstarts>`__ — simple, runnable examples for getting started with many of these drivers
