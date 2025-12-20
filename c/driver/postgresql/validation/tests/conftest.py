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

import sys
from pathlib import Path

import adbc_drivers_validation.model
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    manual_test,
    noci,
    pytest_addoption,
    pytest_collection_modifyitems,
)

from .postgresql import PostgreSQLQuirks


@pytest.fixture(scope="session")
def driver(request) -> adbc_drivers_validation.model.DriverQuirks:
    driver = request.param
    assert driver.startswith("postgresql:")
    return PostgreSQLQuirks()


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    # Library can be in multiple possible locations
    # base = c/driver/postgresql
    base = Path(__file__).parent.parent.parent

    possible_paths = [
        # 1. c/build/driver/postgresql/ (CMake build from c/ directory)
        base.parent.parent / f"build/driver/{driver.name}/libadbc_driver_{driver.name}.{ext}",
        # 2. <repo-root>/build/driver/postgresql/ (CI build location)
        base.parent.parent.parent / f"build/driver/{driver.name}/libadbc_driver_{driver.name}.{ext}",
        # 3. c/driver/postgresql/build/ (local CMake build from driver dir)
        base / f"build/libadbc_driver_{driver.name}.{ext}",
        # 4. c/driver/postgresql/ (direct build output in driver dir)
        base / f"libadbc_driver_{driver.name}.{ext}",
    ]

    for path in possible_paths:
        if path.exists():
            return str(path)

    return str(possible_paths[0])
