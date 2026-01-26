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

============
Related Work
============

In the initial proposal, a survey of existing solutions and systems was
included, which is partially reproduced below for context, though note the
descriptions are only kept up-to-date on a best-effort basis.

Preexisting database client APIs
================================

:external:doc:`Arrow Flight SQL <format/FlightSql>`
  A standard building on top of Arrow Flight, defining how to use
  Flight to talk to databases, retrieve metadata, execute queries, and
  so on. Provides a single client in C++ and Java language that talks
  to any database servers implementing the protocol. Models its API
  surface (though not API design) after JDBC and ODBC.

`DBI for R <https://www.r-dbi.org/>`_
  An R package/ecosystem of packages for database access. Provides a
  single interface with "backends" for specific databases.  While
  row-oriented, `integration with Arrow is under consideration`_,
  including a sketch of effectively the same idea as ADBC.

`JDBC <https://jcp.org/en/jsr/detail?id=221>`_
  A Java library for database access, providing row-based
  APIs. Provides a single interface with drivers for specific
  databases.

`ODBC <https://github.com/microsoft/ODBC-Specification>`_
  A language-agnostic standard from the ISO/IEC for database access,
  associated with Microsoft. Feature-wise, it is similar to JDBC (and
  indeed JDBC can wrap ODBC drivers), but it offers columnar data
  support through fetching buffers of column values (with some
  caveats). Provides a single C interface with drivers for specific
  databases.

`PEP 249 <https://www.python.org/dev/peps/pep-0249/>`_ (DBAPI 2.0)
  A Python standard for database access providing row-based APIs. Not
  a singular package, but rather a set of interfaces that packages
  implement.

Preexisting libraries
=====================

These are libraries which either 1) implement columnar data access for
a particular system; or 2) could be used to implement such access.

:external:doc:`Arrow Flight <format/Flight>`
  An RPC framework optimized for transferring Arrow record batches,
  with application-specific extension points but without any higher
  level semantics.

:external+arrowjava:doc:`Arrow JDBC <jdbc>`
  A Java submodule, part of Arrow/Java, that uses the JDBC API to
  produce Arrow data. Internally, it can read data only row-at-a-time.

`arrow-odbc <https://github.com/pacman82/arrow-odbc>`_
  A Rust community project that uses the ODBC API to produce Arrow
  data, using ODBC’s buffer-based API to perform bulk copies. (See
  also: Turbodbc.)

`Arrowdantic <https://github.com/jorgecarleitao/arrowdantic/>`_
  Python bindings for an implementation of ODBC<>Arrow in Rust.

`pgeon <https://github.com/0x0L/pgeon>`_
  A client that manually parses the Postgres wire format and produces
  Arrow data, bypassing JDBC/ODBC. While it attempts to optimize this
  case, the Postgres wire protocol is still row-oriented.

`Turbodbc <https://turbodbc.readthedocs.io/en/latest/>`_
  A set of Python ODBC bindings, implementing PEP 249, that also
  provides APIs to fetch data as Arrow batches, optimizing the
  conversion internally.

Papers
======

Raasveldt, Mark, and Hannes Mühleisen. `“Don't Hold My Data Hostage -
A Case for Client Protocol Redesign”`_. In *Proceedings of the VLDB
Endowment*, 1022–1033, 2017.

.. _“Don't Hold My Data Hostage - A Case for Client Protocol Redesign”: https://ir.cwi.nl/pub/26415
.. _integration with Arrow is under consideration: https://r-dbi.github.io/dbi3/articles/dbi3.html#using-arrowparquet-as-an-exchange-format
