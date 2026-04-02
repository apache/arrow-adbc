# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Low-level ADBC bindings for the Flight SQL driver."""

import enum
import functools
import typing

import adbc_driver_manager

from ._version import __version__  # noqa:F401

__all__ = [
    "ConnectionOptions",
    "DatabaseOptions",
    "OAuthFlowType",
    "OAuthTokenType",
    "StatementOptions",
    "connect",
]


class OAuthFlowType(enum.Enum):
    """
    OAuth 2.0 flow types supported by the Flight SQL driver.

    Use these values with :attr:`DatabaseOptions.OAUTH_FLOW`.
    """

    #: OAuth 2.0 Client Credentials flow (RFC 6749 Section 4.4).
    #:
    #: Use when the client application needs to authenticate itself
    #: to the authorization server using its own credentials.
    CLIENT_CREDENTIALS = "client_credentials"

    #: OAuth 2.0 Token Exchange flow (RFC 8693).
    #:
    #: Use when the client application wants to exchange one
    #: security token for another.
    TOKEN_EXCHANGE = "token_exchange"


class OAuthTokenType(enum.Enum):
    """
    OAuth 2.0 token types supported for token exchange (RFC 8693).

    Use these values with token type options like
    :attr:`DatabaseOptions.OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE`,
    :attr:`DatabaseOptions.OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE`, and
    :attr:`DatabaseOptions.OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE`.
    """

    #: An OAuth 2.0 access token.
    ACCESS_TOKEN = "urn:ietf:params:oauth:token-type:access_token"
    #: An OAuth 2.0 refresh token.
    REFRESH_TOKEN = "urn:ietf:params:oauth:token-type:refresh_token"
    #: An OpenID Connect ID token.
    ID_TOKEN = "urn:ietf:params:oauth:token-type:id_token"
    #: A SAML 1.1 assertion.
    SAML1 = "urn:ietf:params:oauth:token-type:saml1"
    #: A SAML 2.0 assertion.
    SAML2 = "urn:ietf:params:oauth:token-type:saml2"
    #: A JSON Web Token (JWT).
    JWT = "urn:ietf:params:oauth:token-type:jwt"


class DatabaseOptions(enum.Enum):
    """Database options specific to the Flight SQL driver."""

    #: The authorization header to use for requests.
    AUTHORIZATION_HEADER = "adbc.flight.sql.authorization_header"
    #: Server name in authentication handshake
    AUTHORITY = "adbc.flight.sql.client_option.authority"
    #: Enable mTLS and use these PEM-encoded certificates.
    MTLS_CERT_CHAIN = "adbc.flight.sql.client_option.mtls_cert_chain"
    #: Enable mTLS and use this PEM-encoded private key.
    MTLS_PRIVATE_KEY = "adbc.flight.sql.client_option.mtls_private_key"
    #: Add an arbitrary header to all outgoing requests.
    #:
    #: This option should prefix the name of the header to add
    #: (i.e. it should be used like
    #: ``f"{DatabaseOptions.RpcCallHeaderPrefix}.x-my-header"``).
    RPC_CALL_HEADER_PREFIX = "adbc.flight.sql.rpc.call_header."
    #: Set a timeout on calls that fetch data (in floating-point seconds).
    #:
    #: This corresponds to Flight RPC DoGet calls.
    TIMEOUT_FETCH = "adbc.flight.sql.rpc.timeout_seconds.fetch"
    #: Set a timeout on calls that execute queries (in floating-point
    #: seconds).
    #:
    #: This corresponds to Flight RPC GetFlightInfo calls.
    TIMEOUT_QUERY = "adbc.flight.sql.rpc.timeout_seconds.query"
    #: Set a timeout on calls that upload or update data (in
    #: floating-point seconds).
    TIMEOUT_UPDATE = "adbc.flight.sql.rpc.timeout_seconds.update"
    #: Override the hostname used for TLS.
    TLS_OVERRIDE_HOSTNAME = "adbc.flight.sql.client_option.tls_override_hostname"
    #: Use these PEM-encoded root certificates for TLS.
    TLS_ROOT_CERTS = "adbc.flight.sql.client_option.tls_root_certs"
    #: Do not verify the server's TLS certificate.
    TLS_SKIP_VERIFY = "adbc.flight.sql.client_option.tls_skip_verify"
    #: Block and wait for the connection to be established.
    WITH_BLOCK = "adbc.flight.sql.client_option.with_block"
    #: Enable cookie middleware. Default is disabled ("false")
    WITH_COOKIE_MIDDLEWARE = "adbc.flight.sql.rpc.with_cookie_middleware"
    #: Set the maximum gRPC message size (in bytes). The default is 16 MiB.
    WITH_MAX_MSG_SIZE = "adbc.flight.sql.client_option.with_max_msg_size"

    # OAuth 2.0 options

    #: Specifies the OAuth 2.0 flow type to use.
    #:
    #: See :class:`OAuthFlowType` for possible values.
    OAUTH_FLOW = "adbc.flight.sql.oauth.flow"
    #: The authorization endpoint URL for OAuth 2.0.
    OAUTH_AUTH_URI = "adbc.flight.sql.oauth.auth_uri"
    #: The endpoint URL where the client application requests tokens
    #: from the authorization server.
    OAUTH_TOKEN_URI = "adbc.flight.sql.oauth.token_uri"
    #: The redirect URI for OAuth 2.0 flows.
    OAUTH_REDIRECT_URI = "adbc.flight.sql.oauth.redirect_uri"
    #: Space-separated list of permissions that the client is requesting
    #: access to (e.g., ``"read.all offline_access"``).
    OAUTH_SCOPE = "adbc.flight.sql.oauth.scope"
    #: Unique identifier issued to the client application by the
    #: authorization server.
    OAUTH_CLIENT_ID = "adbc.flight.sql.oauth.client_id"
    #: Secret associated with the client_id. Used to authenticate the
    #: client application to the authorization server.
    OAUTH_CLIENT_SECRET = "adbc.flight.sql.oauth.client_secret"

    # OAuth 2.0 Token Exchange options (RFC 8693)

    #: The security token that the client application wants to exchange.
    OAUTH_EXCHANGE_SUBJECT_TOKEN = "adbc.flight.sql.oauth.exchange.subject_token"
    #: Identifier for the type of the subject token.
    #:
    #: See :class:`OAuthTokenType` for supported token types.
    OAUTH_EXCHANGE_SUBJECT_TOKEN_TYPE = (
        "adbc.flight.sql.oauth.exchange.subject_token_type"
    )
    #: A security token that represents the identity of the acting party.
    OAUTH_EXCHANGE_ACTOR_TOKEN = "adbc.flight.sql.oauth.exchange.actor_token"
    #: Identifier for the type of the actor token.
    #:
    #: See :class:`OAuthTokenType` for supported token types.
    OAUTH_EXCHANGE_ACTOR_TOKEN_TYPE = "adbc.flight.sql.oauth.exchange.actor_token_type"
    #: The type of token the client wants to receive in exchange.
    #:
    #: See :class:`OAuthTokenType` for supported token types.
    OAUTH_EXCHANGE_REQUESTED_TOKEN_TYPE = (
        "adbc.flight.sql.oauth.exchange.requested_token_type"
    )
    #: Specific permissions requested for the new token in token exchange.
    OAUTH_EXCHANGE_SCOPE = "adbc.flight.sql.oauth.exchange.scope"
    #: The intended audience for the requested security token in token exchange.
    OAUTH_EXCHANGE_AUD = "adbc.flight.sql.oauth.exchange.aud"
    #: The resource server where the client intends to use the requested
    #: security token in token exchange.
    OAUTH_EXCHANGE_RESOURCE = "adbc.flight.sql.oauth.exchange.resource"


class ConnectionOptions(enum.Enum):
    """Connection options specific to the Flight SQL driver."""

    #: Add an arbitrary header to all outgoing requests.
    #:
    #: This option should prefix the name of the header to add
    #: (i.e. it should be used like
    #: ``f"{ConnectionOptions.RPC_CALL_HEADER_PREFIX}x-my-header"``).
    #:
    #: Overrides any headers set via the equivalent database option.
    RPC_CALL_HEADER_PREFIX = DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
    #: Get all session options as a JSON key-value blob.
    OPTION_SESSION_OPTIONS = "adbc.flight.sql.session.options"
    #: Get or set a session option.
    OPTION_SESSION_OPTION_PREFIX = "adbc.flight.sql.session.option."
    #: Erase a session option (use "" as the value).
    OPTION_ERASE_SESSION_OPTION_PREFIX = "adbc.flight.sql.session.optionerase."
    #: Get or set a boolean valued session option.
    OPTION_BOOL_SESSION_OPTION_PREFIX = "adbc.flight.sql.session.optionbool."
    #: Get or set a string-list-valued session option as a JSON array.
    OPTION_STRING_LIST_SESSION_OPTION_PREFIX = (
        "adbc.flight.sql.session.optionstringlist."
    )
    #: Set a timeout on calls that fetch data (in floating-point seconds).
    #:
    #: This corresponds to Flight RPC DoGet calls.
    TIMEOUT_FETCH = DatabaseOptions.TIMEOUT_FETCH.value
    #: Set a timeout on calls that execute queries (in floating-point
    #: seconds).
    #:
    #: This corresponds to Flight RPC GetFlightInfo calls.
    TIMEOUT_QUERY = DatabaseOptions.TIMEOUT_QUERY.value
    #: Set a timeout on calls that upload or update data (in
    #: floating-point seconds).
    TIMEOUT_UPDATE = DatabaseOptions.TIMEOUT_UPDATE.value


class StatementOptions(enum.Enum):
    """Statement options specific to the Flight SQL driver."""

    #: The latest FlightInfo value.
    #:
    #: Thread-safe.  Mostly useful when using incremental execution, where an
    #: advanced client may want to inspect the latest FlightInfo from the
    #: service, but without waiting for execute_partitions to return.  (The
    #: service may send an updated FlightInfo with progress/app_metadata
    #: values, but execute_partitions will only return if there are new
    #: endpoints.)
    LAST_FLIGHT_INFO = "adbc.flight.sql.statement.exec.last_flight_info"
    #: The number of batches to queue per partition. Defaults to 5.
    #:
    #: This controls how much we read ahead on result sets.
    QUEUE_SIZE = "adbc.rpc.result_queue_size"
    #: Add an arbitrary header to all outgoing requests.
    #:
    #: This option should prefix the name of the header to add
    #: (i.e. it should be used like
    #: ``f"{ConnectionOptions.RPC_CALL_HEADER_PREFIX}x-my-header"``).
    #:
    #: Overrides any headers set via the equivalent database or
    #: connection options.
    RPC_CALL_HEADER_PREFIX = DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
    #: Set the Substrait version passed in the Flight SQL request.
    #:
    #: Most servers will not make use of this since the Substrait
    #: specification was updated to embed the version in the plan
    #: itself after this was originally added to Flight SQL.
    SUBSTRAIT_VERSION = "adbc.flight.sql.substrait.version"
    #: Set a timeout on calls that fetch data (in floating-point seconds).
    #:
    #: This corresponds to Flight RPC DoGet calls.
    TIMEOUT_FETCH = DatabaseOptions.TIMEOUT_FETCH.value
    #: Set a timeout on calls that execute queries (in floating-point
    #: seconds).
    #:
    #: This corresponds to Flight RPC GetFlightInfo calls.
    TIMEOUT_QUERY = DatabaseOptions.TIMEOUT_QUERY.value
    #: Set a timeout on calls that upload or update data (in
    #: floating-point seconds).
    TIMEOUT_UPDATE = DatabaseOptions.TIMEOUT_UPDATE.value


def connect(
    uri: str, db_kwargs: typing.Optional[typing.Dict[str, str]] = None
) -> adbc_driver_manager.AdbcDatabase:
    """
    Create a low level ADBC connection to a Flight SQL backend.

    Parameters
    ----------
    uri : str
        The URI to connect to.
    db_kwargs : dict, optional
        Initial database connection parameters.
    """
    return adbc_driver_manager.AdbcDatabase(
        driver=_driver_path(), uri=uri, **(db_kwargs or {})
    )


@functools.lru_cache
def _driver_path() -> str:
    import pathlib
    import sys

    import importlib_resources

    driver = "adbc_driver_flightsql"

    # Wheels bundle the shared library
    root = importlib_resources.files(driver)
    # The filename is always the same regardless of platform
    entrypoint = root.joinpath(f"lib{driver}.so")
    if entrypoint.is_file():
        return str(entrypoint)

    # Search sys.prefix + '/lib' (Unix, Conda on Unix)
    root = pathlib.Path(sys.prefix)
    for filename in (f"lib{driver}.so", f"lib{driver}.dylib"):
        entrypoint = root.joinpath("lib", filename)
        if entrypoint.is_file():
            return str(entrypoint)

    # Conda on Windows
    entrypoint = root.joinpath("bin", f"{driver}.dll")
    if entrypoint.is_file():
        return str(entrypoint)

    # Let the driver manager fall back to (DY)LD_LIBRARY_PATH/PATH
    # (It will insert 'lib', 'so', etc. as needed)
    return driver
