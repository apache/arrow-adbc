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

=============================
Concurrency and Thread Safety
=============================

In general, objects allow serialized access from multiple threads: one
thread may make a call, and once finished, another thread may make a
call.  They do not allow concurrent access from multiple threads.

Somewhat related is the question of overlapping/concurrent execution
of multi-step operations, from a single thread or multiple threads.
For example, two AdbcStatement objects can be created from the same
AdbcConnection:

.. code-block:: c

   struct AdbcStatement stmt1;
   struct AdbcStatement stmt2;

   struct ArrowArrayStream out1;
   struct ArrowArrayStream out2;

   /* Ignoring error handling for brevity */
   AdbcStatementNew(&conn, &stmt1, NULL);
   AdbcStatementNew(&conn, &stmt2, NULL);
   AdbcStatementSetSqlQuery(&stmt1, "SELECT * FROM a", NULL);
   AdbcStatementSetSqlQuery(&stmt2, "SELECT * FROM b", NULL);

   AdbcStatementExecuteQuery(&stmt1, &out1, NULL, NULL);
   AdbcStatementExecuteQuery(&stmt2, &out2, NULL, NULL);
   /* What happens to the result set of stmt1? */

What happens if the client application calls
:c:func:`AdbcStatementExecuteQuery` on ``stmt1``, then on ``stmt2``,
without reading the result set of ``stmt1``?  Some existing client
libraries/protocols, like libpq, don't support concurrent execution of
queries from a single connection.  So the driver would have to
either 1) buffer all results into memory during the first ``Execute``
(or otherwise allow the program to continue reading the first result
set), 2) issue an error on the second ``Execute``, or 3) invalidate
the first statement's result set on the second ``Execute``.

In this case, ADBC allows drivers to choose 1) or 2).  If possible and
reasonable, the driver should allow concurrent execution, whether
because the underlying protocol is designed for it or by buffering
result sets.  But the driver is allowed to error if it is not possible
to support it.

Another use case is having a single statement, but executing it
multiple times and reading the result sets concurrently.  A client
might desire to do this with a prepared statement, for instance:

.. code-block:: c

   /* Ignoring error handling for brevity */
   struct AdbcStatement stmt;
   AdbcStatementNew(&conn, &stmt, NULL);
   AdbcStatementSetSqlQuery(&stmt, "SELECT * FROM a WHERE foo > ?", NULL);
   AdbcStatementPrepare(&stmt, NULL);

   struct ArrowArrayStream stream;
   AdbcStatementBind(&stmt, &array1, &schema, NULL);
   AdbcStatementExecuteQuery(&stmt, &stream, NULL, NULL);
   /* Spawn a thread to process `stream` */

   struct ArrowArrayStream stream2;
   AdbcStatementBind(&stmt, &array2, &schema, NULL);
   AdbcStatementExecuteQuery(&stmt, &stream2, NULL, NULL);
   /* What happens to `stream` here? */

ADBC chooses to disallow this (specifically: the second call to
``Execute`` must invalidate the result set of the first call), in line
with existing APIs that generally do not support 'overlapping' usage
of a single prepared statement in this way.
