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

:sd_hide_title:

=================
Apache Arrow ADBC
=================

.. div::
   :style: border-bottom: 1px solid var(--color-foreground-border);

   .. grid::
      :margin: 4 3 0 0

      .. grid-item::
         :columns: 12 12 12 12
         :class: sd-fs-2

         ADBC: Arrow Database Connectivity

      .. grid-item::
         :columns: 12 12 12 12
         :class: sd-fs-4

         **Cross-language**, **Arrow-native** database access.

ADBC is a standardized set of APIs and libraries for fast, Arrow-native
database access. Execute SQL queries, fetch results, ingest bulk data,
explore catalogs, and more. ADBC uses Arrow columnar format to eliminate
copies and conversions, speeding up data retrieval for analytics and AI
applications.

Getting Started
---------------

.. grid:: 1 1 2 3
   :margin: 4 4 0 0
   :gutter: 3

   .. grid-item-card:: :octicon:`plug` Available Drivers
      :link: driver/index
      :link-type: doc

      Connect to a growing list of databases using ADBC drivers.

   .. grid-item-card:: :octicon:`code` Client Libraries
      :link: client_libraries
      :link-type: doc

      Use ADBC from C/C++, C#, Go, Java, JavaScript, Python, R, Ruby, and Rust.

   .. grid-item-card:: :octicon:`tools` Writing Drivers
      :link: driver/authoring
      :link-type: doc

      Create your own ADBC driver using the driver SDKs available in multiple languages.


.. _dbc: https://docs.columnar.tech/dbc
.. _Substrait: https://substrait.io/

Why ADBC?
---------

.. grid:: 1 2 2 2
   :margin: 4 4 0 0
   :gutter: 1

   .. grid-item-card:: Arrow-native
      :link: https://arrow.apache.org/

      Run queries and get back results in Arrow columnar format,
      eliminating copies and conversions for blazing-fast transfer
      to and from Arrow-native databases and engines.

      +++
      Learn about Apache Arrow

   .. grid-item-card:: Multi-database
      :link: driver/index
      :link-type: doc

      Connect to all kinds of databases through one vendor-neutral standard,
      even ones that aren't Arrow-native. ADBC drivers optimize conversion
      to/from Arrow where required.

      +++
      See Supported Drivers

   .. grid-item-card:: Cross-language
      :link: client_libraries
      :link-type: doc

      Work in C/C++, C#, Go, Java, JavaScript / TypeScript, Python, R, Ruby,
      Rust, and more. Use idiomatic APIs like DBAPI in Python,
      ``database/sql`` in Go, and DBI in R.

      +++
      See Client Libraries

   .. grid-item-card:: Full-featured
      :link: format/comparison
      :link-type: doc

      Execute SQL and Substrait, query database catalogs, inspect table
      schemas, and more.  ADBC handles common tasks without having to pull in
      another database client.

      +++
      Compare to Other APIs

More Resources
--------------

.. grid:: 1 2 2 2
   :margin: 4 4 0 0
   :gutter: 2

   .. grid-item-card:: Explore

      .. button-ref:: integrations
         :ref-type: doc
         :color: secondary
         :expand:

         Tools & Integrations

      .. button-ref:: connection_profiles
         :ref-type: doc
         :color: secondary
         :expand:

         Connection Profiles

      .. button-link:: https://arrow.apache.org/blog/2023/01/05/introducing-arrow-adbc/
         :color: secondary
         :expand:

         Introducing ADBC :octicon:`cross-reference`

      .. button-ref:: faq
         :ref-type: doc
         :color: secondary
         :expand:

         FAQ

      .. button-ref:: glossary
         :ref-type: doc
         :color: secondary
         :expand:

         Glossary

   .. grid-item-card:: Community

      .. button-link:: https://github.com/apache/arrow-adbc/issues
         :color: secondary
         :expand:

         :fab:`github` Issues / Questions

      .. button-link:: https://arrow.apache.org/community/
         :color: secondary
         :expand:

         Mailing List :octicon:`cross-reference`

      .. button-link:: https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md
         :color: secondary
         :expand:

         Contributing :octicon:`cross-reference`

      .. button-link:: https://adbc-drivers.org
         :color: secondary
         :expand:

         Driver Foundry :octicon:`cross-reference`

.. toctree::
   :maxdepth: 1
   :titlesonly:
   :caption: Connect
   :hidden:

   Client Libraries <client_libraries>
   Drivers <driver/index>
   Tools & Integrations <integrations>
   Connection Profiles <connection_profiles>

.. toctree::
   :maxdepth: 1
   :caption: Build
   :hidden:

   Writing New Drivers <driver/authoring>

.. toctree::
   :maxdepth: 1
   :caption: Supported Environments
   :hidden:

   C/C++ <cpp/index>
   C#/.NET <csharp/index>
   Go <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>
   Java <java/index>
   JavaScript <javascript/index>
   Python <python/index>
   R <r/index>
   Ruby <ruby/index>
   Rust <rust/index>
   Installation <installation>

.. toctree::
   :maxdepth: 1
   :caption: Specification
   :hidden:

   format/specification
   format/versioning
   format/comparison
   format/how_manager
   format/driver_manifests
   format/connection_profiles
   format/related_work

.. toctree::
   :maxdepth: 1
   :caption: Contribute
   :hidden:

   development/contributing
   development/nightly
   development/versioning
   development/releasing

.. toctree::
   :maxdepth: 1
   :caption: Resources
   :hidden:

   faq
   glossary
   genindex
