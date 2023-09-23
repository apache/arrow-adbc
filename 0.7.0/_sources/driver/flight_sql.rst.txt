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

**Available for:** C/C++, GLib/Ruby, Go, Java, Python

The Flight SQL Driver provides access to any database implementing a
:doc:`arrow:format/FlightSql` compatible endpoint.

Installation
============

.. tab-set::

   .. tab-item:: C/C++
      :sync: cpp

      For conda-forge users:

      .. code-block:: shell

         mamba install libadbc-driver-flightsql

   .. tab-item:: Go
      :sync: go

      .. code-block:: shell

         go get github.com/apache/arrow-adbc/go/adbc

   .. tab-item:: Java
      :sync: java

      Add a dependency on ``org.apache.arrow.adbc:adbc-driver-flight-sql``.

      For Maven users:

      .. code-block:: xml

         <dependency>
           <groupId>org.apache.arrow.adbc</groupId>
           <artifactId>adbc-driver-flight-sql</artifactId>
         </dependency>

   .. tab-item:: Python
      :sync: python

      .. code-block:: shell

         # For conda-forge
         mamba install adbc-driver-flightsql

         # For pip
         pip install adbc_driver_flightsql

   .. tab-item:: R
      :sync: r

      .. code-block:: r

         # install.packages("pak")
         pak::pak("apache/arrow-adbc/r/adbcflightsql")

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
         AdbcDatabaseSetOption(&database, "driver", "adbc_driver_flightsql", nullptr);
         AdbcDatabaseSetOption(&database, "uri", "grpc://localhost:8080", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. code-block:: python

         from adbc_driver_flightsql import DatabaseOptions
         from adbc_driver_flightsql.dbapi import connect

         headers = {"foo": "bar"}

         with connect(
             "grpc+tls://localhost:8080",
             db_kwargs={
                 DatabaseOptions.AUTHORIZATION_HEADER.value: "Bearer <token>",
                 DatabaseOptions.TLS_SKIP_VERIFY.value: "true",
                 **{
                     f"{DatabaseOptions.RPC_CALL_HEADER_PREFIX.value}{k}": v
                     for k, v in headers.items()
                 },
             }
         ) as conn:
             pass

   .. tab-item:: Go
      :sync: go

      .. code-block:: go

         import (
            "context"

            "github.com/apache/arrow-adbc/go/adbc"
            "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
         )

         var headers = map[string]string{"foo": "bar"}

         func main() {
            options := map[string]string{
                adbc.OptionKeyURI: "grpc+tls://localhost:8080",
                flightsql.OptionSSLSkipVerify: adbc.OptionValueEnabled,
            }

            for k, v := range headers {
                options[flightsql.OptionRPCCallHeaderPrefix + k] = v
            }

            var drv flightsql.Driver
            db, err := drv.NewDatabase(options)
            if err != nil {
                // do something with the error
            }

            cnxn, err := db.Open(context.Background())
            if err != nil {
                // handle the error
            }
            defer cnxn.Close()
         }

Supported Features
==================

The Flight SQL driver generally supports features defined in the ADBC
API specification 1.0.0, as well as some additional, custom options.

.. warning:: The Java driver does not support all options here.  See
             `issue #745
             <https://github.com/apache/arrow-adbc/issues/745>`_.

Authentication
--------------

The driver does no authentication by default.  The driver implements a
few optional authentication schemes:

- Mutual TLS (mTLS): see "Client Options" below.
- An HTTP-style scheme mimicking the Arrow Flight SQL JDBC driver.

  Set the options ``username`` and ``password`` on the
  :cpp:class:`AdbcDatabase`.  Alternatively, set the option
  ``adbc.flight.sql.authorization_header`` for full control.

  The client provides credentials sending an ``authorization`` from
  client to server.  The server then responds with an
  ``authorization`` header on the first request.  The value of this
  header will then be sent back as the ``authorization`` header on all
  future requests.

Bulk Ingestion
--------------

Flight SQL does not have a dedicated API for bulk ingestion of Arrow
data into a given table.  The driver does not currently implement bulk
ingestion as a result.

Client Options
--------------

The options used for creating the Flight RPC client can be customized.
These options map 1:1 with the options in FlightClientOptions:

``adbc.flight.sql.client_option.mtls_cert_chain``
    The certificate chain to use for mTLS.

``adbc.flight.sql.client_option.mtls_private_key``
    The private key to use for mTLS.

``adbc.flight.sql.client_option.tls_override_hostname``
    Override the hostname used to verify the server's TLS certificate.

``adbc.flight.sql.client_option.tls_skip_verify``
    Disable verification of the server's TLS certificate.  Value
    should be ``true`` or ``false``.

``adbc.flight.sql.client_option.tls_root_certs``
    Override the root certificates used to validate the server's TLS
    certificate.

``adbc.flight.sql.client_option.with_block``
    Whether connections should wait until connections are established,
    or connect lazily when used.  The latter is gRPC's default
    behavior, but the driver defaults to eager connection to surface
    errors earlier.  Value should be ``true`` or ``false``.

``adbc.flight.sql.client_option.with_max_msg_size``
    The maximum message size to accept from the server.  The driver
    defaults to 16 MiB since Flight services tend to return larger
    reponse payloads.  Should be a positive integer number of bytes.

``adbc.flight.sql.rpc.with_cookie_middleware``
    Enable or disable middleware that processes and handles "set-cookie"
    metadata headers returned from the server and sends "Cookie" headers
    back from the client. Value should be ``true`` or ``false``. Default
    is ``false``.

Custom Call Headers
-------------------

Custom HTTP headers can be attached to requests via options that apply
to :cpp:class:`AdbcDatabase`, :cpp:class:`AdbcConnection`, and
:cpp:class:`AdbcStatement`.

``adbc.flight.sql.rpc.call_header.<HEADER NAME>``
  Add the header ``<HEADER NAME>`` to outgoing requests with the given
  value.

  .. warning:: Header names must be in all lowercase.

Distributed Result Sets
-----------------------

The driver will fetch all partitions (FlightEndpoints) returned by the
server, in an unspecified order (note that Flight SQL itself does not
define an ordering on these partitions).  If an endpoint has no
locations, the data will be fetched using the original server
connection.  Else, the driver will try each location given, in order,
until a request succeeds.  If the connection or request fails, it will
try the next location.

The driver does not currently cache or pool these secondary
connections.  It also does not retry connections or requests.

All partitions are fetched in parallel.  A limited number of batches
are queued per partition.  Data is returned to the client in the order
of the partitions.

The queue size can be changed by setting an option on the
:cpp:class:`AdbcStatement`:

``adbc.rpc.result_queue_size``
    The number of batches to queue per partition.  Defaults to 5.

Metadata
--------

The driver currently will not populate column constraint info (foreign
keys, primary keys, etc.) in :cpp:func:`AdbcConnectionGetObjects`.
Also, catalog filters are evaluated as simple string matches, not
``LIKE``-style patterns.

Partitioned Result Sets
-----------------------

The Flight SQL driver supports ADBC's partitioned result sets.  When
requested, each partition of a result set contains a serialized
FlightInfo, containing one of the FlightEndpoints of the original
response.  Clients who may wish to introspect the partition can do so
by deserializing the contained FlightInfo from the ADBC partitions.
(For example, a client that wishes to distribute work across multiple
workers or machines may want to try to take advantage of locality
information that ADBC does not have.)

.. TODO: code samples

Timeouts
--------

By default, timeouts are not used for RPC calls.  They can be set via
special options on :cpp:class:`AdbcConnection`.  In general, it is
best practice to set timeouts to avoid unexpectedly getting stuck.
The options are as follows:

``adbc.flight.sql.rpc.timeout_seconds.fetch``
    A timeout (in floating-point seconds) for any API calls that fetch
    data.  This corresponds to Flight ``DoGet`` calls.

    For example, this controls the timeout of the underlying Flight
    calls that fetch more data as a result set is consumed.

``adbc.flight.sql.rpc.timeout_seconds.query``
    A timeout (in floating-point seconds) for any API calls that
    execute a query.  This corresponds to Flight ``GetFlightInfo``
    calls.

    For example, this controls the timeout of the underlying Flight
    calls that implement :func:`AdbcStatementExecuteQuery`.

``adbc.flight.sql.rpc.timeout_seconds.update``
    A timeout (in floating-point seconds) for any API calls that
    upload data or perform other updates.

    For example, this controls the timeout of the underlying Flight
    calls that implement bulk ingestion, or transaction support.

Transactions
------------

The driver supports transactions.  It will first check the server's
SqlInfo to determine whether this is supported.  Otherwise,
transaction-related ADBC APIs will return
:c:type:`ADBC_STATUS_NOT_IMPLEMENTED`.

.. _DBAPI 2.0: https://peps.python.org/pep-0249/
