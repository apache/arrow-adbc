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

==============
Driver Manager
==============

The driver manager is a library that implements the ADBC API by
delegating to dynamically-loaded drivers.  This allows applications to
use multiple drivers simultaneously, and decouple themselves from the
specific driver.

Installation
============

See :doc:`./install`.

Usage
=====

To create a database, use the :cpp:class:`AdbcDatabase` API as usual,
but during initialization, provide two additional parameters in
addition to the driver-specific connection parameters: ``driver`` and
(optionally) ``entrypoint``.  ``driver`` must be the name of a library
to load, or the path to a library to load. ``entrypoint``, if
provided, should be the name of the symbol that serves as the ADBC
entrypoint (see :cpp:type:`AdbcDriverInitFunc`).

.. code-block:: c

   /* Ignoring error handling */
   struct AdbcDatabase database;
   memset(&database, 0, sizeof(database));
   AdbcDatabaseNew(&database, NULL);
   /* On Linux: loads libadbc_driver_sqlite.so
    * On MacOS: loads libadbc_driver_sqlite.dylib
    * On Windows: loads adbc_driver_sqlite.dll */
   AdbcDatabaseSetOption(&database, "driver", "adbc_driver_sqlite", NULL);
   /* Set additional options for the specific driver */
   AdbcDatabaseInit(&database, NULL);


API Reference
=============

The driver manager includes a few additional functions beyond the ADBC
API.  See the API reference: :doc:`./api/adbc_driver_manager`.
