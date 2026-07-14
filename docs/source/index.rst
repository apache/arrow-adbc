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
Execute SQL queries, insert bulk data, query database catalogs, and more, all
using Arrow data to eliminate unnecessary data copies, speed up access, and
make it more convenient to build analytical applications.

Getting Started
---------------

To connect to a database with ADBC, you need two things:

.. card:: :octicon:`plug` Drivers

   The majority of ADBC drivers are published by :ref:`third-parties <driver-index-third-party>` but :ref:`a few <driver-index-apache>` are available from the ADBC project for historical reasons.

   **Third-Party Driver Vendors**

   - `DuckDB <https://duckdb.org/docs/lts/clients/adbc>`_
   - `ADBC Driver Foundry <https://adbc-drivers.org>`_

   .. NOTE::
      All first and third-party drivers are packaged by a third-party, `Columnar <https://columnar.tech>`_, and can be installed with their command line tool dbc_ by running ``dbc install <driver>``. Visit the `dbc docs <dbc_>`_ to see what drivers are available.

   See :doc:`Drivers<driver/index>` for more information on drivers.

.. _dbc: https://docs.columnar.tech/dbc
.. _Substrait: https://substrait.io/


.. card:: :octicon:`code` Client Libraries

   You can connect to a database with ADBC using a growing set of programming languages:

   .. tab-set::

      .. tab-item:: Python

         .. code-block:: bash

            pip install adbc-driver-manager

         See the :doc:`Python Quickstart <python/quickstart>` for usage instructions.

      .. tab-item:: C/C++

         See the :doc:`C/C++ Quickstart <cpp/quickstart>` for installation and usage instructions.

      .. tab-item:: C#

         .. code-block:: bash

            dotnet add package Apache.Arrow.Adbc.Client

         See the :doc:`C# Quickstart <csharp/quickstart>` for usage instructions.

      .. tab-item:: Go

         .. code-block:: bash

            go get github.com/apache/arrow-adbc/go/adbc

         See the `Go Documentation <https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc>`_ for usage instructions.

      .. tab-item:: Java

         .. code-block:: xml

            <dependency>
            <groupId>org.apache.arrow.adbc</groupId>
            <artifactId>adbc-driver-manager</artifactId>
            </dependency>

         See the :doc:`Java Quickstart <java/quickstart>` for usage instructions.

      .. tab-item:: JS

         .. code-block:: bash

            npm install @apache-arrow/adbc

         See the :doc:`JavaScript Quickstart <javascript/quickstart>` for usage instructions.

      .. tab-item:: R

         .. code-block:: r

            install.packages("adbcdrivermanager")

         See the :doc:`R Quickstart <r/quickstart>` for usage instructions.

      .. tab-item:: Ruby

         .. code-block:: bash

            bundle add adbc

         See the :doc:`Ruby Quickstart <ruby/quickstart>` for usage instructions.

      .. tab-item:: Rust

         .. code-block:: bash

            cargo add arrow-adbc

         See the :doc:`Rust Quickstart <rust/quickstart>` for usage instructions.

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

More
====

(TODO: Fill this in or rework section below)

- Integrations
- Building Drivers
- Specification

Old Content
===========

(TODO: Rework this)

.. grid:: 1 2 3 4
   :margin: 4 4 0 0
   :gutter: 2

   .. grid-item-card::
      :columns: 12 4 4 4

      Connect
      ^^^^^^^

      Connect to many different databases with ADBC using your language(s) of choice.

      +++

      .. button-ref:: client_libraries
            :ref-type: doc
            :color: secondary
            :expand:

            Client Libraries

      .. button-ref:: driver/index
            :ref-type: doc
            :color: secondary
            :expand:

            Drivers

      .. button-ref:: integrations
            :ref-type: doc
            :color: secondary
            :expand:

            Integrations

      .. button-ref:: connection_profiles
            :ref-type: doc
            :color: secondary
            :expand:

            Connection Profiles

   .. grid-item-card::
      :columns: 12 4 4 4

      Build
      ^^^^^

      Learn how ADBC drivers are built and learn how to build your own.

      +++

      .. button-ref:: driver/sdk
            :ref-type: doc
            :color: secondary
            :expand:

            Driver SDKs

      .. button-ref:: driver/authoring
            :ref-type: doc
            :color: secondary
            :expand:

            Writing New Drivers

      .. button-ref:: format/specification
         :ref-type: doc
         :color: secondary
         :expand:

         ADBC Specification

   .. grid-item-card::
      :columns: 12 4 4 4

      Learn
      ^^^^^

      Learn more about ADBC.

      +++

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

   .. grid-item-card::
      :columns: 12 4 4 4

      Development
      ^^^^^^^^^^^

      Report bugs, ask questions, and contribute to Apache Arrow.

      +++

      .. button-link:: https://github.com/apache/arrow-adbc/issues
         :color: secondary
         :expand:

         :fab:`github` Issues/Questions

      .. button-link:: https://arrow.apache.org/community/
         :color: secondary
         :expand:

         Mailing List :octicon:`cross-reference`

      .. button-link:: https://github.com/apache/arrow-adbc/blob/main/CONTRIBUTING.md
         :color: secondary
         :expand:

         Contributing :octicon:`cross-reference`

.. toctree::
   :maxdepth: 1
   :caption: Connect
   :hidden:

   Client Libraries <client_libraries>
   Drivers <driver/index>
   Integrations <integrations>
   Connection Profiles <connection_profiles>

.. toctree::
   :maxdepth: 1
   :caption: Build
   :hidden:

   Driver SDKs <driver/sdk>
   Driver Manifests <format/driver_manifests>
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
   Ruby <ruby/index>
   Rust <rust/index>

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
