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

"""Low-level ADBC bindings for the IBM DB2 driver."""

import enum
import functools
import typing

import adbc_driver_manager

from ._version import __version__  # noqa:F401

__all__ = ["ConnectionOptions", "StatementOptions", "__version__", "connect"]


class ConnectionOptions(enum.Enum):
    """Connection options specific to the DB2 driver."""

    #: The database name.
    DATABASE = "adbc.db2.database"
    #: The hostname of the DB2 server.
    HOSTNAME = "adbc.db2.hostname"
    #: The port number of the DB2 server.
    PORT = "adbc.db2.port"
    #: The username for authentication.
    UID = "adbc.db2.uid"
    #: The password for authentication.
    PWD = "adbc.db2.pwd"


class StatementOptions(enum.Enum):
    """Statement options specific to the DB2 driver."""

    #: The number of rows per batch.  Defaults to 65536.
    BATCH_ROWS = "adbc.db2.query.batch_rows"


def connect(
    uri: typing.Optional[str] = None,
    *,
    db_kwargs: typing.Optional[typing.Dict[str, str]] = None,
) -> adbc_driver_manager.AdbcDatabase:
    """
    Create a low level ADBC connection to IBM DB2.

    Parameters
    ----------
    uri
        The DB2 connection string, e.g.
        ``DATABASE=mydb;HOSTNAME=host;PORT=50000;PROTOCOL=TCPIP;UID=user;PWD=pass;``
    db_kwargs
        Additional key-value options passed to ``AdbcDatabase``.
    """
    kwargs: typing.Dict[str, typing.Any] = {"driver": _driver_path()}
    if uri is not None:
        kwargs["uri"] = uri
    if db_kwargs:
        kwargs.update(db_kwargs)
    return adbc_driver_manager.AdbcDatabase(**kwargs)


@functools.lru_cache
def _driver_path() -> str:
    import os
    import pathlib
    import sys

    import importlib_resources

    driver = "adbc_driver_db2"

    env_lib = os.environ.get("ADBC_DB2_LIBRARY")
    if env_lib:
        p = pathlib.Path(env_lib).expanduser()
        if p.is_file():
            return str(p.resolve())

    # Wheels bundle the shared library (macOS often uses .dylib; some layouts use .so)
    root = importlib_resources.files(driver)
    names = []
    if sys.platform == "darwin":
        names.append(f"lib{driver}.dylib")
    names.append(f"lib{driver}.so")
    if sys.platform != "darwin":
        names.append(f"lib{driver}.dylib")
    for name in names:
        entrypoint = root.joinpath(name)
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
    return driver
