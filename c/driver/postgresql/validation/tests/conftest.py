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
import adbc_drivers_validation.tests.conftest
import pytest
from adbc_drivers_validation.tests.conftest import (  # noqa: F401
    conn,
    conn_factory,
    db_kwargs,
    manual_test,
    noci,
    pytest_collection_modifyitems,
)

from .postgresql import VENDORS, get_quirks


def pytest_addoption(parser) -> None:
    adbc_drivers_validation.tests.conftest.pytest_addoption(parser)
    parser.addoption(
        "--vendor",
        choices=VENDORS,
        default="postgresql",
        help="PostgreSQL-compatible database vendor to validate",
    )


@pytest.fixture(scope="session")
def driver(request) -> adbc_drivers_validation.model.DriverQuirks:
    return get_quirks(request.config.getoption("vendor"))


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
        base.parent.parent / f"build/driver/postgresql/libadbc_driver_postgresql.{ext}",
        # 2. <repo-root>/build/driver/postgresql/ (CI build location)
        base.parent.parent.parent
        / f"build/driver/postgresql/libadbc_driver_postgresql.{ext}",
        # 3. c/driver/postgresql/build/ (local CMake build from driver dir)
        base / f"build/libadbc_driver_postgresql.{ext}",
        # 4. c/driver/postgresql/ (direct build output in driver dir)
        base / f"libadbc_driver_postgresql.{ext}",
    ]

    for path in possible_paths:
        if path.exists():
            return str(path)

    return str(possible_paths[0])


@pytest.fixture(scope="session", autouse=True)
def _setup_backend(request, conn_factory) -> None:  # noqa: F811
    with conn_factory() as conn:  # noqa: F811
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS secondary")
