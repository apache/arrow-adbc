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

import functools
import typing

import adbc_driver_manager

from ._version import __version__

__all__ = ["connect", "__version__"]


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

    # Wheels bundle the shared library
    root = importlib.resources.files(__package__)
    # The filename is always the same regardless of platform
    entrypoint = root.joinpath("libadbc_driver_flightsql.so")
    if entrypoint.is_file():
        return str(entrypoint)

    # Search sys.prefix + '/lib' (Unix, Conda on Unix)
    root = pathlib.Path(sys.prefix)
    for filename in ("libadbc_driver_flightsql.so", "libadbc_driver_flightsql.dylib"):
        entrypoint = root.joinpath('lib', filename)
        if entrypoint.is_file():
            return str(entrypoint)

    # Conda on Windows
    entrypoint = root.joinpath('bin', "adbc_driver_flightsql.dll")
    if entrypoint.is_file():
        return str(entrypoint)

    # Fall back to (DY)LD_LIBRARY_PATH/PATH
    return "adbc_driver_flightsql"
