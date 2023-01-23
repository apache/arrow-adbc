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
import importlib.resources

import adbc_driver_manager

from ._version import __version__

__all__ = ["connect", "__version__"]


def connect(uri: str) -> adbc_driver_manager.AdbcDatabase:
    """Create a low level ADBC connection to PostgreSQL."""
    return adbc_driver_manager.AdbcDatabase(driver=_driver_path(), uri=uri)


@functools.cache
def _driver_path() -> str:
    root = importlib.resources.files(__package__)
    entrypoint = root.joinpath("libadbc_driver_postgresql.so")
    if entrypoint.is_file():
        return str(entrypoint)
    is_conda = root.joinpath(".is_conda")
    if is_conda.is_file():
        with is_conda.open() as source:
            return source.read().strip()
    raise RuntimeError(f"Could not find driver, was {__package__} properly installed?")


_driver_path()
