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


@pytest.fixture
def dremio_uri():
    dremio_uri = os.environ.get("ADBC_DREMIO_FLIGHTSQL_URI")
    if not dremio_uri:
        pytest.skip("Set ADBC_DREMIO_FLIGHTSQL_URI to run tests")
    yield dremio_uri


@pytest.fixture
def dremio(dremio_uri):
    username = os.environ.get("ADBC_DREMIO_FLIGHTSQL_USER")
    password = os.environ.get("ADBC_DREMIO_FLIGHTSQL_PASS")
    with adbc_driver_flightsql.connect(
        dremio_uri,
        db_kwargs={
            "username": username,
            "password": password,
        },
    ) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


@pytest.fixture
def dremio_dbapi(dremio_uri):
    username = os.environ.get("ADBC_DREMIO_FLIGHTSQL_USER")
    password = os.environ.get("ADBC_DREMIO_FLIGHTSQL_PASS")
    with adbc_driver_flightsql.dbapi.connect(
        dremio_uri,
        db_kwargs={
            "username": username,
            "password": password,
        },
    ) as conn:
        yield conn
