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



The driver manager is a package that implements the ADBC Go API by delegating to
dynamically-loaded drivers. It allows using multiple drivers simultaneously,
including drivers not written in Go.

Installation
============

.. code-block:: shell

   go get github.com/apache/arrow-adbc/go/adbc/drivermgr

Usage
=====

To manually create a connection: first, create a `adbc.Database`_,
passing ``driver`` and (optionally) ``entrypoint``.  ``driver`` must be the
name of a library to load, or the path to a library to load.  ``entrypoint``,
if provided, should be the name of the symbol that serves as the ADBC
entrypoint (see :c:type:`AdbcDriverInitFunc`).  Then, create a
`adbc.Connection`_.


.. _adbc.Database: https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc#Database
.. _adbc.Connection: https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc#Connection

.. code-block:: go

   import (
       "context"
       "github.com/apache/arrow-adbc/go/adbc"
       "github.com/apache/arrow-adbc/go/adbc/drivermgr"
   )

   ctx := context.Background()

   // Create driver and database
	var drv drivermgr.Driver
   db, err := drv.NewDatabase(map[string]string{
       "driver": "adbc_driver_sqlite",
   })
   if err != nil {
       return err
   }
   defer db.Close()

   // Open connection
   conn, err := db.Open(ctx)
   if err != nil {
       return err
   }
   defer conn.Close()

API Reference
=============

See the API reference: https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc.
