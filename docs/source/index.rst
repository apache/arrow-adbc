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

=================
Apache Arrow ADBC
=================

ADBC (Arrow Database Connectivity) is an API specification for
Arrow-based database access.  It provides a set of APIs in C, Go, and
Java that define how to interact with databases, including executing
queries and fetching metadata, that use Arrow data for result sets and
query parameters.  These APIs are then implemented by drivers (or a
driver manager) that use some underlying protocol to work with
specific databases.

ADBC aims to provide applications with a single API to work with
multiple databases, both Arrow-native and not.  Application code
should not need to juggle conversions from non-Arrow-native
datasources alongside bindings for multiple Arrow-native database
protocols.  And Arrow Flight SQL by itself cannot solve this problem,
because it's a specific wire protocol that not all databases will
implement.

.. toctree::
   :maxdepth: 1
   :caption: Specifications:

   format/specification
   format/versioning
   format/comparison

.. toctree::
   :maxdepth: 1
   :caption: Supported Environments:

   C/C++ <cpp/index>
   Java <java/index>
   Python <python/index>

.. toctree::
   :maxdepth: 1
   :caption: Contributing:

   contributing
   nightly

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
