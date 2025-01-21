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

============================
Versioning and Compatibility
============================

The goal is to be **ABI-compatible** across releases.  Hence, a few
choices were made:

- Most structures do not contain embedded fields or functions, but
  instead use free functions, making it easy to add new functions.
- Enumerations are defined via ``typedef``/``#define``.

Of course, we can never add/remove/change struct members, and we can
never change the signatures of existing functions.

In ADBC 1.1.0, it was decided this would only apply to the "public"
API, and not the driver-internal API (:c:struct:`AdbcDriver`).  New
members were added to this struct in the 1.1.0 revision.
Compatibility is handled as follows:

The driver entrypoint, :c:type:`AdbcDriverInitFunc`, is given a
version and a pointer to a table of function pointers to initialize
(the :c:struct:`AdbcDriver`).  The size of the table will depend on
the version; when a new version of ADBC is accepted, then a new table
of function pointers may be expanded.  For each version, the driver
knows the expected size of the table, and must not read/write fields
beyond that size.  If/when we add a new ADBC version, the following
scenarios are possible:

- An updated client application uses an old driver library.  The
  client will pass a `version` field greater than what the driver
  recognizes, so the driver will return
  :c:macro:`ADBC_STATUS_NOT_IMPLEMENTED` and the client can decide
  whether to abort or retry with an older version.
- An old client application uses an updated driver library.  The
  client will pass a ``version`` lower than what the driver
  recognizes, so the driver can either error, or if it can still
  implement the old API contract, initialize the subset of the table
  corresponding to the older version.

This approach does not let us change the signatures of existing
functions, but we can add new functions and remove existing ones.

Versioning
==========

ADBC is versioned separately from the core Arrow project.  The API
standard and components (driver manager, drivers) are also versioned
separately, but both follow semantic versioning.

For example: components may make backwards-compatible releases as
1.0.0, 1.0.1, 1.1.0, 1.2.0, etc.  They may release
backwards-incompatible versions such as 2.0.0, but which still
implement the API standard version 1.0.0.

Similarly, this documentation describes the ADBC API standard version
1.1.0.  If/when a compatible revision is made (e.g. new standard
options or API functions are defined), the next version would be
1.2.0.  If incompatible changes are made (e.g. changing the signature
or semantics of a function), the next version would be 2.0.0.
