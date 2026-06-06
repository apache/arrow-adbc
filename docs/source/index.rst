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

ADBC is a set of APIs and libraries for Arrow-native access to databases.
Execute SQL and Substrait_ queries, query database catalogs, and more, all
using Arrow data to eliminate unnecessary data copies, speed up access, and
make it more convenient to build analytical applications.

.. _Substrait: https://substrait.io/

.. grid:: 1
   :margin: 4 4 0 0
   :gutter: 1

   .. grid-item-card::

         Client Libraries
         ^^^

         Connect to many commmon databases with your language of choice.
         +++

         .. button-ref:: cpp/quickstart
            :ref-type: doc
            :color: info

            Overview

         .. button-ref:: cpp/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            C/C++

         .. button-ref:: csharp/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            C#/.NET

         .. button-link:: https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc
            :color: secondary
            :class: adbc-inline-btn

            Go

         .. button-ref:: java/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            Java

         .. button-ref:: javascript/index
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            JavaScript

         .. button-ref:: python/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            Python

         .. button-ref:: r/index
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            R

         .. button-ref:: rust/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            Rust

   .. grid-item-card::

         Drivers
         ^^^

         Learn more about which databases you can connect to with ADBC.
         +++

         .. button-ref:: driver/index
            :ref-type: doc
            :color: info

            Drivers

   .. grid-item-card::

         Driver SDKs
         ^^^

         Build drivers in your language of choice, usable by client libraries in any language.
         +++

         .. button-ref:: cpp/quickstart
            :ref-type: doc
            :color: info

            Overview

         .. button-ref:: cpp/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            C/C++

         .. button-ref:: csharp/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            C#/.NET

         .. button-link:: https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc
            :color: secondary
            :class: adbc-inline-btn

            Go

         .. button-ref:: rust/quickstart
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            Rust

   .. grid-item-card::

         Specification
         ^^^

         Learn about the underlying API specification.

         +++

         .. button-link:: https://arrow.apache.org/blog/2023/01/05/introducing-arrow-adbc/
            :color: secondary
            :class: adbc-inline-btn

            Introducing ADBC :octicon:`cross-reference`

         .. button-ref:: format/specification
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            Specification

         .. button-ref:: faq
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            FAQ

         .. button-ref:: glossary
            :ref-type: doc
            :color: secondary
            :class: adbc-inline-btn

            Glossary

   .. grid-item-card::

         Development
         ^^^

         Report bugs, ask questions, and contribute to Apache Arrow.

         +++

         .. button-link:: https://github.com/apache/arrow-adbc/issues
            :color: secondary
            :class: adbc-inline-btn

            :fab:`github` Issues/Questions

         .. button-link:: https://arrow.apache.org/community/
            :color: secondary
            :class: adbc-inline-btn

            Mailing List :octicon:`cross-reference`

         .. button-link:: https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md
            :color: secondary
            :class: adbc-inline-btn

            Contributing :octicon:`cross-reference`

Why ADBC?
=========

.. grid:: 1 2 2 2
   :margin: 4 4 0 0
   :gutter: 1

   .. grid-item-card:: Arrow-native
      :link: https://arrow.apache.org/

      Execute queries and get back results in Arrow format, eliminating extra
      data copies for Arrow-native backends.

      +++
      Learn about Apache Arrow

   .. grid-item-card:: Backend-agnostic
      :link: driver/status
      :link-type: doc

      Connect to all kinds of databases, even ones that aren't Arrow-native.
      ADBC drivers optimize conversion to/from Arrow where required, saving
      work for developers.

      +++
      See Supported Drivers

   .. grid-item-card:: Cross-language

      Work in C/C++, C#, Go, Java, Python, R, Ruby, Rust, and more.

   .. grid-item-card:: Full-featured

      Execute SQL and Substrait, query database catalogs, inspect table
      schemas, and more.  ADBC handles common tasks without having to pull in
      another database client.

   .. grid-item-card:: Language-native

      Use language-native APIs that you're already familiar with, like DBAPI
      in Python, ``database/sql`` in Go, or DBI in R.

.. toctree::
   :maxdepth: 1
   :hidden:

   faq
   glossary
   genindex

.. toctree::
   :maxdepth: 1
   :caption: Connect
   :hidden:

   Client Libraries <status_client>
   Drivers <driver/index>

.. toctree::
   :maxdepth: 1
   :caption: Build
   :hidden:

   Driver SDKs <driver/sdk>
   driver/authoring

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
   :caption: Supported Environments
   :hidden:

   C/C++ <cpp/index>
   C#/.NET <csharp/index>
   Go <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>
   Java <java/index>
   JavaScript <javascript/index>
   Python <python/index>
   R <r/index>
   Rust <rust/index>

.. toctree::
   :maxdepth: 1
   :caption: Contribute
   :hidden:

   development/contributing
   development/nightly
   development/versioning
   development/releasing
