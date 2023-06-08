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
ADBC API Standard
=================

This document summarizes the general featureset.

- For C/C++ details, see :doc:`adbc.h <../../cpp/api/adbc>`.
- For Go details, see the `source <https://github.com/apache/arrow-adbc/blob/main/go/adbc/adbc.go>`__.
- For Java details, see the `source <https://github.com/apache/arrow-adbc/tree/main/java/core>`__.

Databases
=========

Databases hold state shared by multiple connections.  Generally, this
means common configuration and caches.  For in-memory databases, it
provides a place to hold ownership of the in-memory database.

- C/C++: :cpp:class:`AdbcDatabase`
- Go: ``Driver``
- Java: ``org.apache.arrow.adbc.core.AdbcDatabase``

Connections
===========

A connection is a single, logical connection to a database.

- C/C++: :cpp:class:`AdbcConnection`
- Go: ``Connection``
- Java: ``org.apache.arrow.adbc.core.AdbcConnection``

Autocommit
----------

By default, connections are expected to operate in autocommit mode;
that is, queries take effect immediately upon execution.  This can be
disabled in favor of manual commit/rollback calls, but not all
implementations will support this.

- C/C++: :c:macro:`ADBC_CONNECTION_OPTION_AUTOCOMMIT`
- Go: ``OptionKeyAutoCommit``
- Java: ``org.apache.arrow.adbc.core.AdbcConnection#setAutoCommit(boolean)``

Statements
==========

Statements hold state related to query execution.  They represent both
one-off queries and prepared statements.  They can be reused, though
doing so will invalidate prior result sets from that statement.  (See
:doc:`../../cpp/concurrency`.)

- C/C++: :cpp:class:`AdbcStatement`
- Go: ``Statement``
- Java: ``org.apache.arrow.adbc.core.AdbcStatement``

.. _specification-bulk-ingestion:

Bulk Ingestion
--------------

ADBC provides explicit facilities to ingest batches of Arrow data into
a database table.  For databases which support it, this can avoid
overheads from the typical bind-insert loop.  Also, this (mostly)
frees the user from knowing the right SQL syntax for their database.

- C/C++: :c:macro:`ADBC_INGEST_OPTION_TARGET_TABLE` and related
  options.
- Go: ``OptionKeyIngestTargetTable``
- Java: ``org.apache.arrow.adbc.core.AdbcConnection#bulkIngest(String, org.apache.arrow.adbc.core.BulkIngestMode)``

Partitioned Result Sets
-----------------------

ADBC lets a driver explicitly expose partitioned and/or distributed
result sets to clients.  (This is similar to functionality in Flight
RPC/Flight SQL.)  Clients may take advantage of this to distribute
computations on a result set across multiple threads, processes, or
machines.

- C/C++: :cpp:func:`AdbcStatementExecutePartitions`
- Go: ``Statement.ExecutePartitions``
- Java: ``org.apache.arrow.adbc.core.AdbcStatement#executePartitioned()``

Lifecycle & Usage
-----------------

.. image:: AdbcStatement.svg
   :alt: The lifecycle of a statement.
   :width: 100%

Basic Usage
~~~~~~~~~~~

.. mermaid:: AdbcStatementBasicUsage.mmd
   :caption: Preparing the statement and binding parameters are optional.

Consuming Result Sets
~~~~~~~~~~~~~~~~~~~~~

.. mermaid:: AdbcStatementConsumeResultSet.mmd
   :caption: This is equivalent to reading from what many Arrow
             libraries call a RecordBatchReader.

Bulk Data Ingestion
~~~~~~~~~~~~~~~~~~~

.. mermaid:: AdbcStatementBulkIngest.mmd
   :caption: There is no need to prepare the statement.

Update-only Queries (No Result Set)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. mermaid:: AdbcStatementUpdate.mmd
   :caption: Preparing the statement and binding parameters are optional.

Partitioned Execution
~~~~~~~~~~~~~~~~~~~~~

.. mermaid:: AdbcStatementPartitioned.mmd
   :caption: This is similar to fetching data in Arrow Flight RPC (by
             design). See :doc:`"Downloading Data" <arrow:format/Flight>`.
