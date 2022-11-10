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
Flight SQL Driver
=================

The Flight SQL Driver provides access to any database implementing a
:doc:`arrow:format/FlightSql` compatible endpoint.

Installation
============

The Flight SQL driver is shipped as part of the Arrow C++ libraries
and PyArrow.  See :ref:`Installation <cpp-install-flight-sql>`.

Usage
=====

To connect to a database, supply the "uri" parameter when constructing
the :cpp:class:`AdbcDatabase`.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include "adbc.h"

         // Ignoring error handling
         struct AdbcDatabase database;
         AdbcDatabaseNew(&database, nullptr);
         AdbcDatabaseSetOption(&database, "uri", "grpc://localhost:8080", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         import pyarrow.flight_sql


         with pyarrow.flight_sql.connect("grpc://localhost:8080") as conn:
             pass

Additional Configuration Options
--------------------------------

The Flight SQL driver supports some additional configuration options
in addition to the "standard" ADBC options.

Custom Call Headers
~~~~~~~~~~~~~~~~~~~

Custom HTTP headers can be attached to requests via options that apply
to both :cpp:class:`AdbcConnection` and :cpp:class:`AdbcStatement`.

``arrow.flight.sql.rpc.call_header.<HEADER NAME>``
  Add the header ``<HEADER NAME>`` to outgoing requests with the given
  value.

  .. warning:: Header names must be in all lowercase.

Timeouts
~~~~~~~~

By default, timeouts are not used for RPC calls.  They can be set via
special options on :cpp:class:`AdbcConnection`.  In general, it is
best practice to set timeouts to avoid unexpectedly getting stuck.
The options are as follows:

``arrow.flight.sql.rpc.timeout_seconds.fetch``
    A timeout (in floating-point seconds) for any API calls that fetch
    data.  This corresponds to Flight ``DoGet`` calls.

    For example, this controls the timeout of the underlying Flight
    calls that fetch more data as a result set is consumed.

``arrow.flight.sql.rpc.timeout_seconds.query``
    A timeout (in floating-point seconds) for any API calls that
    execute a query.  This corresponds to Flight ``GetFlightInfo``
    calls.

    For example, this controls the timeout of the underlying Flight
    calls that implement :func:`AdbcStatementExecuteQuery`.

``arrow.flight.sql.rpc.timeout_seconds.update``
    A timeout (in floating-point seconds) for any API calls that
    upload data or perform other updates.

    For example, this controls the timeout of the underlying Flight
    calls that implement bulk ingestion, or transaction support.

.. TODO: code samples

Type Mapping
~~~~~~~~~~~~

When executing a bulk ingestion operation, the driver needs to be able
to construct appropriate SQL queries for the database.  (The driver
does not currently support using Substrait plans instead.)  In
particular, a mapping from Arrow types to SQL type names is required.
While a default mapping is provided, the client may wish to override
this mapping, which can be done by setting special options on
:cpp:class:`AdbcDatabase`.  (The driver does not currently inspect
Flight SQL metadata to construct this mapping.)

All such options begin with ``arrow.flight.sql.quirks.ingest_type.``
and are followed by a type name below.

.. csv-table:: Type Names
   :header: "Arrow Type Name", "Default SQL Type Name"

   binary,BLOB
   bool,BOOLEAN
   date32,DATE
   date64,DATE
   decimal128,NUMERIC
   decimal256,NUMERIC
   double,DOUBLE PRECISION
   float,REAL
   int16,SMALLINT
   int32,INT
   int64,BIGINT
   large_binary,BLOB
   large_string,TEXT
   string,TEXT
   time32,TIME
   time64,TIME
   timestamp,TIMESTAMP

.. TODO: code samples

Partitioned Result Set Support
------------------------------

The Flight SQL driver supports ADBC's partitioned result sets, mapping
them onto FlightEndpoints.  Each partition of a result set contains a
serialized FlightInfo, containing one of the FlightEndpoints of the
original response.  Clients who may wish to introspect the partition
can do so by deserializing the contained FlightInfo from the ADBC
partitions.  (For example, a client that wishes to distribute work
across multiple workers or machines may want to try to take advantage
of locality information that ADBC does not have.)

.. TODO: code samples

.. _DBAPI 2.0: https://peps.python.org/pep-0249/
