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

.. adbc_driver_status:: ../../../c/driver/flightsql/README.md

The Flight SQL Driver provides access to any database implementing a
:doc:`arrow:format/FlightSql` compatible endpoint.

Installation
============

.. adbc_driver_installation:: ../../../c/driver/flightsql/README.md

Usage
=====

To connect to a database, supply the "uri" parameter when constructing
the :c:struct:`AdbcDatabase`.

.. tab-set::

   .. tab-item:: C++
      :sync: cpp

      .. code-block:: cpp

         #include "arrow-adbc/adbc.h"

         // Ignoring error handling
         struct AdbcDatabase database;
         AdbcDatabaseNew(&database, nullptr);
         AdbcDatabaseSetOption(&database, "driver", "adbc_driver_flightsql", nullptr);
         AdbcDatabaseSetOption(&database, "uri", "grpc://localhost:8080", nullptr);
         AdbcDatabaseInit(&database, nullptr);

   .. tab-item:: Python
      :sync: python

      .. note:: For detailed examples, see :doc:`../python/recipe/flight_sql`.

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

      .. recipe:: ../../../go/adbc/driver/flightsql/example_usage_test.go

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
  :c:struct:`AdbcDatabase`.  Alternatively, set the option
  ``adbc.flight.sql.authorization_header`` for full control.

  The client provides credentials sending an ``authorization`` from
  client to server.  The server then responds with an
  ``authorization`` header on the first request.  The value of this
  header will then be sent back as the ``authorization`` header on all
  future requests.

- OAuth 2.0 authentication flows.

  The client provides :ref:`configurations <oauth-configurations>` to allow client application to obtain access
  tokens from an authorization server. The obtained token is then used
  on the ``authorization`` header on all future requests.

Bulk Ingestion
--------------

Flight SQL does not have a dedicated API for bulk ingestion of Arrow
data into a given table.  The driver does not currently implement bulk
ingestion as a result.

Client Options
--------------

The options used for creating the Flight RPC client can be customized.

.. note:: Many of these options simply wrap a gRPC option.  For more details
          about what these options do, consult the `gRPC documentation
          <https://pkg.go.dev/google.golang.org/grpc>`_.

``adbc.flight.sql.client_option.authority``
    Override gRPC's ``:authority`` pseudo-header.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.AUTHORITY`

``adbc.flight.sql.client_option.mtls_cert_chain``
    The certificate chain to use for mTLS.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.MTLS_CERT_CHAIN`

``adbc.flight.sql.client_option.mtls_private_key``
    The private key to use for mTLS.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.MTLS_PRIVATE_KEY`

``adbc.flight.sql.client_option.tls_override_hostname``
    Override the hostname used to verify the server's TLS certificate.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.TLS_OVERRIDE_HOSTNAME`

``adbc.flight.sql.client_option.tls_root_certs``
    Override the root certificates used to validate the server's TLS
    certificate.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.TLS_ROOT_CERTS`

``adbc.flight.sql.client_option.tls_skip_verify``
    Disable verification of the server's TLS certificate.  Value
    should be ``true`` or ``false``.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.TLS_SKIP_VERIFY`

``adbc.flight.sql.client_option.with_block``
    .. warning:: This option is deprecated as gRPC itself has deprecated the
                 underlying option.

    This option has no effect and will be removed in a future release.
    Value should be ``true`` or ``false``.

``adbc.flight.sql.client_option.with_max_msg_size``
    The maximum message size to accept from the server.  The driver
    defaults to 16 MiB since Flight services tend to return larger
    response payloads.  Should be a positive integer number of bytes.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.WITH_MAX_MSG_SIZE`

``adbc.flight.sql.authorization_header``
    Directly specify the value of the ``authorization`` header to send on all
    requests.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.AUTHORIZATION_HEADER`

``adbc.flight.sql.rpc.with_cookie_middleware``
    Enable or disable middleware that processes and handles "set-cookie"
    metadata headers returned from the server and sends "Cookie" headers
    back from the client. Value should be ``true`` or ``false``. Default
    is ``false``.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.WITH_COOKIE_MIDDLEWARE`

Custom Call Headers
-------------------

Custom HTTP headers can be attached to requests via options that apply
to :c:struct:`AdbcDatabase`, :c:struct:`AdbcConnection`, and
:c:struct:`AdbcStatement`.

``adbc.flight.sql.rpc.call_header.<HEADER NAME>``
  Add the header ``<HEADER NAME>`` to outgoing requests with the given
  value.

  Python: :attr:`adbc_driver_flightsql.ConnectionOptions.RPC_CALL_HEADER_PREFIX`

  .. warning:: Header names must be in all lowercase.


OAuth 2.0 Options
-----------------------
.. _oauth-configurations:

Supported configurations to obtain tokens using OAuth 2.0 authentication flows.

``adbc.flight.sql.oauth.flow``
    Specifies the OAuth 2.0 flow type to use. Possible values: ``client_credentials``, ``token_exchange``

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_FLOW`,
    :class:`adbc_driver_flightsql.OAuthFlowType`

``adbc.flight.sql.oauth.client_id``
    Unique identifier issued to the client application by the authorization server

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_CLIENT_ID`

``adbc.flight.sql.oauth.client_secret``
    Secret associated to the client_id. Used to authenticate the client application to the authorization server

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_CLIENT_SECRET`

``adbc.flight.sql.oauth.token_uri``
    The endpoint URL where the client application requests tokens from the authorization server

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_TOKEN_URI`

``adbc.flight.sql.oauth.scope``
    Space-separated list of permissions that the client is requesting access to (e.g ``"read.all offline_access"``)

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_SCOPE`

``adbc.flight.sql.oauth.exchange.subject_token``
    The security token that the client application wants to exchange

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_SUBJECT_TOKEN`

``adbc.flight.sql.oauth.exchange.subject_token_type``
    Identifier for the type of the subject token.
    Check list below for supported token types.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE`,
    :class:`adbc_driver_flightsql.OAuthTokenType`

``adbc.flight.sql.oauth.exchange.actor_token``
    A security token that represents the identity of the acting party

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_ACTOR_TOKEN`

``adbc.flight.sql.oauth.exchange.actor_token_type``
    Identifier for the type of the actor token.
    Check list below for supported token types.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE`,
    :class:`adbc_driver_flightsql.OAuthTokenType`

``adbc.flight.sql.oauth.exchange.aud``
    The intended audience for the requested security token

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_AUD`

``adbc.flight.sql.oauth.exchange.resource``
    The resource server where the client intends to use the requested security token

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_RESOURCE`

``adbc.flight.sql.oauth.exchange.scope``
    Specific permissions requested for the new token

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_SCOPE`

``adbc.flight.sql.oauth.exchange.requested_token_type``
    The type of token the client wants to receive in exchange.
    Check list below for supported token types.

    Python: :attr:`adbc_driver_flightsql.DatabaseOptions.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE`,
    :class:`adbc_driver_flightsql.OAuthTokenType`

Supported token types:
  - ``urn:ietf:params:oauth:token-type:access_token``
  - ``urn:ietf:params:oauth:token-type:refresh_token``
  - ``urn:ietf:params:oauth:token-type:id_token``
  - ``urn:ietf:params:oauth:token-type:saml1``
  - ``urn:ietf:params:oauth:token-type:saml2``
  - ``urn:ietf:params:oauth:token-type:jwt``

    Python: :class:`adbc_driver_flightsql.OAuthTokenType`

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

Some behavior can be configured on the :c:struct:`AdbcStatement`:

``adbc.rpc.result_queue_size``
    The number of batches to queue per partition.  Defaults to 5.

    Python: :attr:`adbc_driver_flightsql.StatementOptions.QUEUE_SIZE`

Incremental Execution
---------------------

By setting :c:macro:`ADBC_STATEMENT_OPTION_INCREMENTAL`, you can use
nonblocking execution with this driver.  This changes the behavior of
:c:func:`AdbcStatementExecutePartitions` only.  When enabled,
ExecutePartitions will return every time there are new partitions (in Flight
SQL terms, when there are new FlightEndpoints) from the server, instead of
blocking until the query is complete.

Some behavior can be configured on the :c:struct:`AdbcStatement`:

``adbc.flight.sql.statement.exec.last_flight_info``
    Get the serialized bytes for the most recent ``FlightInfo`` returned by
    the service.  This is a low-level option intended for advanced usage.  It
    is most useful when incremental execution is enabled, for inspecting the
    latest server response without waiting for
    :c:func:`AdbcStatementExecutePartitions` to return.

    Python: :attr:`adbc_driver_flightsql.StatementOptions.LAST_FLIGHT_INFO`

Metadata
--------

The driver currently will not populate column constraint info (foreign
keys, primary keys, etc.) in :c:func:`AdbcConnectionGetObjects`.
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

Sessions
--------

The driver exposes Flight SQL session support via options on the connection.
There is no explicit command to start a new session; it is expected that the
server itself will manage this.  (You will most likely need to enable cookie
support as described above.)  There is no explicit command to close a session;
this is always issued when the connection is closed.

``adbc.flight.sql.session.options``
    Get all options as a JSON blob.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.OPTION_SESSION_OPTIONS`

``adbc.flight.sql.session.option.``
    Get or set a string/numeric session option.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.OPTION_SESSION_OPTION_PREFIX`

``adbc.flight.sql.session.optionerase.``
    Erase a session option.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.OPTION_ERASE_SESSION_OPTION_PREFIX`

``adbc.flight.sql.session.optionbool.``
    Get or set a boolean session option.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.OPTION_BOOL_SESSION_OPTION_PREFIX`

``adbc.flight.sql.session.optionstringlist.``
    Get or set a string list session option.  The contents should be a
    serialized JSON list.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.OPTION_STRING_LIST_SESSION_OPTION_PREFIX`

Timeouts
--------

By default, timeouts are not used for RPC calls.  They can be set via
special options on :c:struct:`AdbcConnection`.  In general, it is
best practice to set timeouts to avoid unexpectedly getting stuck.
The options are as follows:

``adbc.flight.sql.rpc.timeout_seconds.fetch``
    A timeout (in floating-point seconds) for any API calls that fetch
    data.  This corresponds to Flight ``DoGet`` calls.

    For example, this controls the timeout of the underlying Flight
    calls that fetch more data as a result set is consumed.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.TIMEOUT_FETCH`

``adbc.flight.sql.rpc.timeout_seconds.query``
    A timeout (in floating-point seconds) for any API calls that
    execute a query.  This corresponds to Flight ``GetFlightInfo``
    calls.

    For example, this controls the timeout of the underlying Flight
    calls that implement :c:func:`AdbcStatementExecuteQuery`.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.TIMEOUT_QUERY`

``adbc.flight.sql.rpc.timeout_seconds.update``
    A timeout (in floating-point seconds) for any API calls that
    upload data or perform other updates.

    For example, this controls the timeout of the underlying Flight
    calls that implement bulk ingestion, or transaction support.

    Python: :attr:`adbc_driver_flightsql.ConnectionOptions.TIMEOUT_UPDATE`

There is also a timeout that is set on the :c:struct:`AdbcDatabase`:

``adbc.flight.sql.rpc.timeout_seconds.connect``
    A timeout (in floating-point seconds) for establishing a connection.  The
    default is 20 seconds.

Transactions
------------

The driver supports transactions.  It will first check the server's
SqlInfo to determine whether this is supported.  Otherwise,
transaction-related ADBC APIs will return
:c:macro:`ADBC_STATUS_NOT_IMPLEMENTED`.

.. _DBAPI 2.0: https://peps.python.org/pep-0249/
