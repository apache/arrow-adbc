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

======================
Feature Support Matrix
======================

.. list-table:: ADBC Feature Support
   :header-rows: 1

   * - Driver
     - Bulk Ingestion
     - Metadata
     - Partitioned Result Sets
     - Prepared Statements
     - Transactions

   * - SQLite
     - Yes
     - Yes
     - No
     - Yes
     - Yes

Bulk Ingestion
  Creating or appending to a database table from an Arrow table.

Metadata
  Functions like :cpp:func:`AdbcConnectionGetObjects` that get
  metadata about the database catalog, etc.

Partitioned Result Sets
  Being able to read individual chunks of a (generally distributed)
  result set (:cpp:func:`AdbcStatementExecutePartitions`).
