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

import os

import pytest

import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi
import adbc_driver_manager


@pytest.fixture(scope="session")
def dremio_uri() -> str:
    dremio_uri = os.environ.get("ADBC_DREMIO_FLIGHTSQL_URI")
    if not dremio_uri:
        pytest.skip("Set ADBC_DREMIO_FLIGHTSQL_URI to run tests")
    return dremio_uri


@pytest.fixture(scope="session")
def dremio_user() -> str:
    username = os.environ.get("ADBC_DREMIO_FLIGHTSQL_USER")
    if not username:
        pytest.skip("Set ADBC_DREMIO_FLIGHTSQL_USER to run tests")
    return username


@pytest.fixture(scope="session")
def dremio_pass() -> str:
    password = os.environ.get("ADBC_DREMIO_FLIGHTSQL_PASS")
    if not password:
        pytest.skip("Set ADBC_DREMIO_FLIGHTSQL_PASS to run tests")
    return password


@pytest.fixture
def dremio(dremio_uri, dremio_user, dremio_pass):
    with adbc_driver_flightsql.connect(
        dremio_uri,
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: dremio_user,
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: dremio_pass,
        },
    ) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


@pytest.fixture
def dremio_dbapi(dremio_uri, dremio_user, dremio_pass):
    with adbc_driver_flightsql.dbapi.connect(
        dremio_uri,
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: dremio_user,
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: dremio_pass,
        },
        autocommit=True,
    ) as conn:
        yield conn


@pytest.fixture
def test_dbapi():
    uri = os.environ.get("ADBC_TEST_FLIGHTSQL_URI")
    if not uri:
        pytest.skip("Set ADBC_TEST_FLIGHTSQL_URI to run tests")

    with adbc_driver_flightsql.dbapi.connect(
        uri,
        autocommit=True,
    ) as conn:
        yield conn


@pytest.fixture(scope="session")
def gizmosql_uri() -> str:
    uri = os.environ.get("ADBC_GIZMOSQL_URI")
    if not uri:
        pytest.skip("Set ADBC_GIZMOSQL_URI to run tests")
    return uri


@pytest.fixture(scope="session")
def gizmosql_user() -> str:
    username = os.environ.get("ADBC_GIZMOSQL_USER")
    if not username:
        pytest.skip("Set ADBC_GIZMOSQL_USER to run tests")
    return username


@pytest.fixture(scope="session")
def gizmosql_pass() -> str:
    password = os.environ.get("ADBC_GIZMOSQL_PASSWORD")
    if not password:
        pytest.skip("Set ADBC_GIZMOSQL_PASSWORD to run tests")
    return password


@pytest.fixture
def gizmosql(gizmosql_uri, gizmosql_user, gizmosql_pass):
    """Create a low-level ADBC connection to the GizmoSQL server."""
    with adbc_driver_flightsql.connect(
        gizmosql_uri,
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: gizmosql_user,
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: gizmosql_pass,
        },
    ) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


@pytest.fixture
def gizmosql_dbapi(gizmosql_uri, gizmosql_user, gizmosql_pass):
    """Create a DBAPI (PEP 249) connection to the GizmoSQL server."""
    with adbc_driver_flightsql.dbapi.connect(
        gizmosql_uri,
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: gizmosql_user,
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: gizmosql_pass,
        },
        autocommit=True,
    ) as conn:
        yield conn
