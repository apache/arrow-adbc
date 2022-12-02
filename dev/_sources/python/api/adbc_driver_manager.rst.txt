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

.. default-domain:: py

===================
adbc_driver_manager
===================

Low-Level API
=============

.. automodule:: adbc_driver_manager

Constants & Enums
-----------------

.. autoclass:: adbc_driver_manager.AdbcStatusCode
   :members:

.. autoclass:: adbc_driver_manager.GetObjectsDepth
   :members:

.. XXX: Sphinx doesn't seem to be able to parse docstrings out of
   Cython code, so put the descriptions here instead.

   Alternatively: refactor the package into adbc.pxd, _constants.pyx,
   constants.py, etc.?

.. data:: adbc_driver_manager.INGEST_OPTION_MODE

   Whether to append to or create a new table for bulk ingestion.

.. data:: adbc_driver_manager.INGEST_OPTION_MODE_APPEND

   Append to the table for bulk ingestion.

.. data:: adbc_driver_manager.INGEST_OPTION_MODE_CREATE

   Create a new table for bulk ingestion.

.. data:: adbc_driver_manager.INGEST_OPTION_TARGET_TABLE

   The table to create/append to for bulk ingestion.

Classes
-------

.. autoclass:: adbc_driver_manager.AdbcConnection
   :members:

.. autoclass:: adbc_driver_manager.AdbcDatabase
   :members:

.. autoclass:: adbc_driver_manager.AdbcStatement
   :members:

.. autoclass:: adbc_driver_manager.ArrowArrayHandle
   :members:

.. autoclass:: adbc_driver_manager.ArrowArrayStreamHandle
   :members:

.. autoclass:: adbc_driver_manager.ArrowSchemaHandle
   :members:

DBAPI 2.0 API
=============

.. automodule:: adbc_driver_manager.dbapi

Constants & Enums
-----------------

.. autodata:: adbc_driver_manager.dbapi.apilevel
.. autodata:: adbc_driver_manager.dbapi.paramstyle
.. autodata:: adbc_driver_manager.dbapi.threadsafety

.. autodata:: adbc_driver_manager.dbapi.Date
.. autodata:: adbc_driver_manager.dbapi.Time
.. autodata:: adbc_driver_manager.dbapi.Timestamp

.. autodata:: adbc_driver_manager.dbapi.BINARY
.. autodata:: adbc_driver_manager.dbapi.DATETIME
.. autodata:: adbc_driver_manager.dbapi.NUMBER
.. autodata:: adbc_driver_manager.dbapi.ROWID
.. autodata:: adbc_driver_manager.dbapi.STRING

Functions
---------

.. autofunction:: adbc_driver_manager.dbapi.connect
.. autofunction:: adbc_driver_manager.dbapi.DateFromTicks
.. autofunction:: adbc_driver_manager.dbapi.TimeFromTicks
.. autofunction:: adbc_driver_manager.dbapi.TimestampFromTicks

Classes
-------

.. autoclass:: adbc_driver_manager.dbapi.Connection
   :members:
   :exclude-members: DatabaseError, DataError, Error, IntegrityError,
                     InterfaceError, InternalError, NotSupportedError,
                     OperationalError, ProgrammingError, Warning

.. autoclass:: adbc_driver_manager.dbapi.Cursor
   :members:

Exceptions
==========

.. autoexception:: adbc_driver_manager.DatabaseError
.. autoexception:: adbc_driver_manager.DataError
.. autoexception:: adbc_driver_manager.Error
.. autoexception:: adbc_driver_manager.IntegrityError
.. autoexception:: adbc_driver_manager.InterfaceError
.. autoexception:: adbc_driver_manager.InternalError
.. autoexception:: adbc_driver_manager.NotSupportedError
.. autoexception:: adbc_driver_manager.OperationalError
.. autoexception:: adbc_driver_manager.ProgrammingError
.. autoexception:: adbc_driver_manager.Warning
