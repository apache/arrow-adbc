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

==================
Flight SQL Recipes
==================

Some of these recipes are written against a demo Flight SQL service backed by
SQLite.  You can run it yourself as follows:

.. code-block:: shell

   $ go install github.com/apache/arrow-go/v${ARROW_MAJOR_VERSION}/arrow/flight/flightsql/example/cmd/sqlite_flightsql_server@latest
   $ sqlite_flightsql_server -host 0.0.0.0 -port 8080

Other recipes work using the OSS version of Dremio_:

.. code-block:: shell

   $ docker run -p 9047:9047 -p 31010:31010 -p 45678:45678 dremio/dremio-oss

If you have the ADBC repository checked out and Docker Compose installed, you
can use our configuration to run both services:

.. code-block:: shell

   $ docker compose up --detach --wait dremio dremio-init flightsql-sqlite-test

.. _Dremio: https://www.dremio.com/

Connect to an unsecured Flight SQL service
------------------------------------------

.. recipe:: flightsql_sqlite_connect.py

Connect to a Flight SQL service with username and password
----------------------------------------------------------

.. recipe:: flightsql_dremio_connect.py

Set timeouts and other options
------------------------------

.. recipe:: flightsql_sqlite_options.py

Set the max gRPC message size
-----------------------------

.. recipe:: flightsql_sqlite_max_msg_size.py
