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

===========
Driver SDKs
===========

ADBC drivers can be written in C/C++, C#, Go, Java, or Rust.
Because drivers expose a :doc:`standard C ABI <../format/specification>`, a driver written in any of these languages can be used from client libraries in any other language — so the implementation language is largely a matter of ecosystem fit and preference.

.. list-table::
   :header-rows: 1

   * - Language
     - Documentation
     - Notes
   * - C/C++
     - :doc:`../cpp/index`
     - Drivers are loadable by the Driver Manager and usable from all client languages.
   * - C#
     - :doc:`../csharp/index`
     - Can experimentally export drivers for use from other languages via the C ABI.
   * - Go
     - `go/adbc <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`__
     - Recommended for new drivers; easiest to package and distribute. Drivers are loadable by the Driver Manager and usable from all client languages.
   * - Java
     - :doc:`../java/index`
     - Drivers are usable from Java client libraries.
   * - Rust
     - :doc:`../rust/quickstart`
     - API definitions are still maturing. Drivers can be compiled to a C ABI shared library for use from other languages.

Go is generally the recommended choice for new drivers.
C/C++ drivers have historically had dependency conflicts (e.g. with grpcio or pyarrow in Python processes), whereas Go libraries are easier to package and distribute without conflict.

For guidance on how to structure a new driver, see :doc:`authoring`.
