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

===================
Writing New Drivers
===================

Currently, new drivers can be written in C#, C/C++, Go, and Java.  A driver
written in C/C++ or Go can be used from either of those languages, as well as
C#, Python, R, and Ruby.  (C# can experimentally export drivers to the same
set of languages as well.)  The Rust API definitions for ADBC are still under
development, but we plan for them to be on par with C#, C/C++, and Go in this
respect.

It is so far preferable to write new drivers in Go.  C/C++ have had issues
with dependencies and in particular some not uncommon dependencies in that
ecosystem tend to cause conflicts when loaded into Python processes and
elsewhere.  (For example, the ADBC Flight SQL driver was originally written in
C++ but would have conflicted with the grpcio and pyarrow Python packages.)
It also tends to be easier for us to package and distribute additional Go
libraries than it is for C/C++.

In Go, some frameworks are available for driver
authors. `go/adbc/driver/internal/driverbase`_ manages much of the boilerplate
and basic state management for drivers.  `go/adbc/pkg`_ can template out a C
ABI wrapper around the Go driver.  Especially if the driver is planned to go
upstream, we recommend driver authors consider using these frameworks.

.. _go/adbc/driver/internal/driverbase: https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/internal/driverbase
.. _go/adbc/pkg: https://github.com/apache/arrow-adbc/tree/main/go/adbc/pkg
