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

==========================
Comparison with Other APIs
==========================

.. list-table:: Equivalent concepts between ADBC and other APIs
   :header-rows: 1

   * - Concept/API
     - ADBC
     - database/sql (Golang)
     - DBAPI 2.0 (PEP 249)
     - Flight SQL
     - JDBC
     - ODBC

   * - Shared connection state
     - :c:struct:`AdbcDatabase`
     - ``DB``
     - —
     - —
     - —
     - —

   * - Database connection
     - :c:struct:`AdbcConnection`
     - ``Conn``
     - ``Connection``
     - ``FlightSqlClient``
     - ``Connection``
     - ``SQLHANDLE`` (connection)

   * - Query state
     - :c:struct:`AdbcStatement`
     - —
     - ``Cursor``
     - —
     - ``Statement``
     - ``SQLHANDLE`` (statement)

   * - Prepared statement handle
     - :c:struct:`AdbcStatement`
     - Stmt
     - ``Cursor``
     - ``PreparedStatement``
     - ``PreparedStatement``
     - ``SQLHANDLE`` (statement)

   * - Result set
     - ``ArrowArrayStream``
     - ``*Rows``
     - ``Cursor``
     - ``FlightInfo``
     - ``ResultSet``
     - ``SQLHANDLE`` (statement)
