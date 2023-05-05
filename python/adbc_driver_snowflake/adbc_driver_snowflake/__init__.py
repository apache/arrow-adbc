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

"""Low-level ADBC bindings for the Snowflake driver."""

import enum
import functools
import typing

import adbc_driver_manager

from ._version import __version__  # noqa:F401

__all__ = ["DatabaseOptions", "StatementOptions", "connect"]


class DatabaseOptions(enum.Enum):
    """Database options specific to the Flight SQL driver."""

    ACCOUNT = "adbc.snowflake.sql.account"
    APPLICATION_NAME = "adbc.snowflake.sql.client_option.app_name"
    #: specify the OKTAUrl to use for OKTA Authentication
    AUTH_OKTA_URL = "adbc.snowflake.sql.client_option.okta_url"
    #: specify the token to use for OAuth or other forms of authentication
    AUTH_TOKEN = "adbc.snowflake.sql.client_option.auth_token"
    #: Specify auth type to use for snowflake connection based on
    #: what is supported by the snowflake driver. Default is
    #: "auth_snowflake" (use ValueAuth* consts to specify desired
    #: authentication type).
    AUTH_TYPE = "adbc.snowflake.sql.auth_type"
    #: When true, the MFA token is cached in the credential manager. True by default
    #: on Windows/OSX, false for Linux
    CLIENT_REQUEST_MFA_TOKEN = "adbc.snowflake.sql.client_option.cache_mfa_token"
    #: When true, the ID token is cached in the credential manager. True by default
    #: on Windows/OSX, false for Linux
    CLIENT_STORE_TEMP_CRED = "adbc.snowflake.sql.client_option.store_temp_creds"
    #: Timeout for network round trip + reading http response
    #: use format like http://pkg.go.dev/time#ParseDuration such as
    #: "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
    #: but the absolute value will be used.
    CLIENT_TIMEOUT = "adbc.snowflake.sql.client_option.client_timeout"
    DATABASE = "adbc.snowflake.sql.db"
    DISABLE_TELEMETRY = "adbc.snowflake.sql.client_option.disable_telemetry"
    HOST = "adbc.snowflake.sql.uri.host"
    #: JWT expiration after timeout
    #: use format like http://pkg.go.dev/time#ParseDuration such as
    #: "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
    #: but the absolute value will be used.
    JWT_EXPIRE_TIMEOUT = "adbc.snowflake.sql.client_option.jwt_expire_timeout"
    #: specify the RSA private key to use to sign the JWT
    #: this should point to a file containing a PKCS1 private key to be
    #: loaded. Commonly encoded in PEM blocks of type "RSA PRIVATE KEY"
    JWT_PRIVATE_KEY = "adbc.snowflake.sql.client_option.jwt_private_key"
    #: Login retry timeout EXCLUDING network roundtrip and reading http response
    #: use format like http://pkg.go.dev/time#ParseDuration such as
    #: "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
    #: but the absolute value will be used.
    #: enable the session to persist even after the connection is closed
    KEEP_SESSION_ALIVE = "adbc.snowflake.sql.client_option.keep_session_alive"
    LOGIN_TIMEOUT = "adbc.snowflake.sql.client_option.login_timeout"
    OCSP_FAIL_OPEN_MODE = "adbc.snowflake.sql.client_option.ocsp_fail_open_mode"
    PORT = "adbc.snowflake.sql.uri.port"
    PROTOCOL = "adbc.snowflake.sql.uri.protocol"
    REGION = "adbc.snowflake.sql.region"
    #: request retry timeout EXCLUDING network roundtrip and reading http response
    #: use format like http://pkg.go.dev/time#ParseDuration such as
    #: "300ms", "1.5s" or "1m30s". ParseDuration accepts negative values
    #: but the absolute value will be used.
    REQUEST_TIMEOUT = "adbc.snowflake.sql.client_option.request_timeout"
    ROLE = "adbc.snowflake.sql.role"
    SCHEMA = "adbc.snowflake.sql.schema"
    SSL_SKIP_VERIFY = "adbc.snowflake.sql.client_option.tls_skip_verify"
    WAREHOUSE = "adbc.snowflake.sql.warehouse"


class StatementOptions(enum.Enum):
    """Statement options specific to the Snowflake driver."""

    #: The number of rows per batch. Defaults to 1024.
    BATCH_ROWS = "adbc.rpc.result_queue_size"


def connect(
    uri: typing.Optional[str] = None,
    db_kwargs: typing.Optional[typing.Dict[str, str]] = None,
) -> adbc_driver_manager.AdbcDatabase:
    """
    Create a low level ADBC connection to Snowflake.

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

    driver = "adbc_driver_snowflake"

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
