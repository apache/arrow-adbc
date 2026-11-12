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
import uuid
from pathlib import Path

import adbc_drivers_validation.model
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    db_kwargs,
    pytest_collection_modifyitems,
)

# ruff removes this if grouped above
from adbc_drivers_validation.tests.conftest import (
    conn_factory as conn_factory_impl,  # noqa:F401
)

import adbc_driver_manager.dbapi

from .sqlite import SQLiteQuirks

pytest_addoption = adbc_drivers_validation.tests.conftest.pytest_addoption


@pytest.fixture(scope="session")
def driver(request) -> SQLiteQuirks:
    assert request.param.startswith("sqlite:")
    return SQLiteQuirks()


@pytest.fixture(scope="session")
def driver_path(driver: adbc_drivers_validation.model.DriverQuirks) -> str:
    ext = {
        "win32": "dll",
        "darwin": "dylib",
    }.get(sys.platform, "so")
    # Library can be in multiple possible locations
    # base = c/driver/sqlite
    base = Path(__file__).parent.parent.parent

    possible_paths = [
        # 1. c/build/driver/sqlite/ (CMake build from c/ directory)
        base.parent.parent
        / f"build/driver/{driver.name}/libadbc_driver_{driver.name}.{ext}",
        # 2. <repo-root>/build/driver/sqlite/ (CI build location)
        base.parent.parent.parent
        / f"build/driver/{driver.name}/libadbc_driver_{driver.name}.{ext}",
        # 3. c/driver/sqlite/build/ (local CMake build from driver dir)
        base / f"build/libadbc_driver_{driver.name}.{ext}",
        # 4. c/driver/sqlite/ (direct build output in driver dir)
        base / f"libadbc_driver_{driver.name}.{ext}",
    ]

    for path in possible_paths:
        if path.exists():
            return str(path)

    return str(possible_paths[0])


@pytest.fixture(scope="session", autouse=True)
def secondary_database(driver_path):
    uri = f"file:adbc-validation-secondary-{uuid.uuid4().hex}?mode=memory&cache=shared"
    with adbc_driver_manager.dbapi.connect(
        driver=driver_path, db_kwargs={"uri": uri}, autocommit=True
    ):
        yield uri


@pytest.fixture(scope="session")
def conn_factory(conn_factory_impl, secondary_database):  # noqa: F811
    def factory():
        connection = conn_factory_impl()
        try:
            with connection.cursor() as cursor:
                cursor.execute("ATTACH DATABASE ? AS secondary", (secondary_database,))
        except Exception:
            connection.close()
            raise
        return connection

    return factory
