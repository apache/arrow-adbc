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

Interested in building an ADBC driver? You can develop and host a driver
wherever you like—it just needs to implement the :doc:`ADBC standard
<../format/specification>`. That said, the best place to start is the **ADBC
Driver Foundry**, a community project that hosts most ADBC drivers and provides
the frameworks, templates, testing tools, and distribution infrastructure for
building and shipping them. You can develop your driver entirely within the
Foundry, or keep it in your own repository and use the Foundry only for
distribution and other shared infrastructure.

.. button-link:: https://adbc-drivers.org/building-drivers/
   :color: primary

   Building Drivers guide (ADBC Driver Foundry) :octicon:`link-external`

The rest of this page gives a brief orientation. For anything beyond
that—project templates, frameworks, validation tests, packaging, and
distribution—follow the guide above.

.. note::

   The ADBC project itself focuses on the :doc:`ADBC standard
   <../format/specification>` and :doc:`client libraries <../client_libraries>`,
   not on building and distributing drivers. In most cases, new drivers should
   be developed in the ADBC Driver Foundry, not in the ``apache/arrow-adbc``
   repository.

   The ADBC Driver Foundry is an independent community project, separate from
   the Apache Arrow project.

What Language Should I Use?
===========================

Most ADBC drivers are built as shared libraries (``.so`` / ``.dll`` /
``.dylib``) in a portable systems language (like C/C++, Go, or Rust). A
single shared library can be loaded by any :doc:`ADBC client library
<../client_libraries>` and used from any language, so the implementation
language does not affect which languages can use the driver.

- **Go** and **Rust** are the most common choices, and have the most examples,
  templates, and framework tooling available (see below). If you're starting
  fresh, one of these is usually the easiest path.
- **C/C++** also works, but the build is more involved, and C/C++ drivers have
  historically hit dependency conflicts inside host processes.

In principle a shared-library driver can be written in other languages, such as
C# or Java, but in practice this has not been done.

Separately, a driver can be built as a language-specific package that runs
directly on a managed runtime (for example, a NuGet package for .NET or a Maven
package for the JVM) rather than as a loadable shared library. Such a driver can
only be used from that runtime. This is less common, but the Driver Foundry can
accommodate it too.

Driver Frameworks
=================

The ADBC Driver Foundry maintains frameworks for Go and Rust that provide base
implementations of the ADBC interfaces, so you implement only what's specific to
your database:

- **Go**: `driverbase-go <https://github.com/adbc-drivers/driverbase-go>`__
- **Rust**: `driverbase-rs <https://github.com/adbc-drivers/driverbase-rs>`__

For Rust, `template-rs <https://github.com/adbc-drivers/template-rs>`__
generates a ready-to-build driver crate to start from.

.. note::

   A Go ``driverbase`` also exists in this repository, under
   `go/adbc/driver/internal/driverbase
   <https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/internal/driverbase>`__.
   It remains only for historical reasons (the Flight SQL driver depends on it)
   and is not intended for new work. New Go drivers should use `driverbase-go
   <https://github.com/adbc-drivers/driverbase-go>`__ instead.

Why the Driver Foundry, Not This Repository?
============================================

.. _authoring-why-foundry:

Building and distributing driver binaries is a non-goal of the core ADBC
project, and routing every driver through ``apache/arrow-adbc`` bottlenecks
on its maintainers. The Driver Foundry instead gives each driver its own
repository and maintainers, while its shared frameworks and validation suite
keep drivers consistent and standards-conformant. It also handles testing,
security vulnerability scanning and patching, packaging, and distribution—including
prebuilt, signed, notarized binaries on Columnar's
`ADBC driver registry <https://dbc-cdn.columnar.tech/>`__.
Vendors such as dbt Labs and Microsoft build on Foundry-maintained drivers.

The ADBC maintainers generally decline pull requests adding new drivers to
``apache/arrow-adbc`` and direct you instead to the Foundry.

Columnar's ADBC driver registry, like the Foundry, is maintained independently
of the Apache Arrow project.

.. note::

   A small number of drivers (PostgreSQL, SQLite, and Flight SQL) are still
   maintained in this repository for historical reasons. See :doc:`Drivers
   <index>` for the full list of available drivers and who maintains each one.
