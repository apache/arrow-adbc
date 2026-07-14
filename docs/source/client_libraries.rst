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

ADBC client libraries let you connect to databases and execute queries from your language or languages of choice.
Because all ADBC :doc:`drivers <driver/index>` expose the same :doc:`standard C ABI <format/specification>`, a single client library can generally use drivers written any language using the same client API.

The client libraries in a particular language always provide bindings to the ADBC API and most also offer a driver manager.

Driver Managers
===============

...

.. list-table::
   :header-rows: 1

   * - Language
     - Package
     - Documentation
   * - C/C++
     - ``conda install adbc-driver-manager-cpp``
     - :doc:`cpp/index`
   * - C#/.NET
     - ``dotnet add package Apache.Arrow.Adbc``
     - :doc:`csharp/index`
   * - Go
     - ``go get github.com/apache/arrow-adbc/go/adbc``
     - `pkg.go.dev <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`__
   * - Java
     - ``org.apache.arrow.adbc:adbc-driver-manager``
     - :doc:`java/index`
   * - JavaScript
     - ``npm install @apache-arrow/adbc-driver-manager``
     - :doc:`javascript/index`
   * - Python
     - ``pip install adbc_driver_manager``
     - :doc:`python/index`
   * - R
     - ``install.packages("adbcdrivermanager")``
     - :doc:`r/index`
   * - Ruby
     - ``gem install red-adbc`` or ``bundle add red-adbc``
     - :doc:`ruby/index`
   * - Rust
     - ``cargo add adbc_core``
     - :doc:`rust/index`

Bindings
========

.. list-table::
   :header-rows: 1

   * - Language
     - Package
     - Documentation
   * - C/C++
     - Just vendor ``adbc.h``
     - :doc:`cpp/index`
   * - C#/.NET
     - ``dotnet add package Apache.Arrow.Adbc``
     - :doc:`csharp/index`
   * - Go
     - ``go get github.com/apache/arrow-adbc/go/adbc``
     - `pkg.go.dev <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`__
   * - Java
     - ``org.apache.arrow.adbc:adbc-core``
     - :doc:`java/index`
   * - JavaScript
     - ``npm install @apache-arrow/adbc-driver-manager``
     - :doc:`javascript/index`
   * - Python
     - ``pip install adbc_driver_manager``
     - :doc:`python/index`
   * - R
     - ``install.packages("adbcdrivermanager")``
     - :doc:`r/index`
   * - Ruby
     - ``gem install red-adbc`` or ``bundle add red-adbc``
     - :doc:`ruby/index`
   * - Rust
     - ``cargo add adbc_driver_manager``
     - :doc:`rust/index`

Using a Client Library
======================

Client libraries provide a language-native interface on top of the ADBC API.
They handle loading drivers, managing connections, and converting results into idiomatic types for each language.
For example, Python exposes a `DBAPI 2.0 (PEP 249) <https://peps.python.org/pep-0249/>`__-style interface, Go uses ``database/sql``, and R uses DBI.

To use a client library you need both the library itself and a :doc:`driver <driver/index>` for the database you want to connect to.
The :doc:`ADBC Driver Manager <format/how_manager>` handles loading the driver at run-time, so in most languages you install a separate package per driver (e.g. ``adbc_driver_sqlite`` in Python) alongside the client library.
