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
    "StatementOptions",
    "connect",
]


class DatabaseOptions(enum.Enum):
    """Database options specific to the Flight SQL driver."""

    #: The authorization header to use for requests.
    AUTHORIZATION_HEADER = "adbc.flight.sql.authorization_header"
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
    #: Set the maximum gRPC message size (in bytes). The default is 16 MiB.
    WITH_MAX_MSG_SIZE = "adbc.flight.sql.client_option.with_max_msg_size"


class ConnectionOptions(enum.Enum):
    """Connection options specific to the Flight SQL driver."""

    #: Add an arbitrary header to all outgoing requests.
    #:
    #: This option should prefix the name of the header to add
    #: (i.e. it should be used like
    #: ``f"{ConnectionOptions.RpcCallHeaderPrefix}.x-my-header"``).
    #:
    #: Overrides any headers set via the equivalent database option.
    RPC_CALL_HEADER_PREFIX = DatabaseOptions.RPC_CALL_HEADER_PREFIX.value
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

    #: The number of batches to queue per partition. Defaults to 5.
    #:
    #: This controls how much we read ahead on result sets.
    QUEUE_SIZE = "adbc.rpc.result_queue_size"
    #: Add an arbitrary header to all outgoing requests.
    #:
    #: This option should prefix the name of the header to add
    #: (i.e. it should be used like
    #: ``f"{ConnectionOptions.RpcCallHeaderPrefix}.x-my-header"``).
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


@functools.cache
def _driver_path() -> str:
    import importlib.resources
    import pathlib
    import sys

    driver = "adbc_driver_flightsql"

    # Wheels bundle the shared library
    root = importlib.resources.files(__package__)
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
