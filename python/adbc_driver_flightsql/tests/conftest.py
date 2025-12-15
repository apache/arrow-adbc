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
import time

import docker
import pytest

import adbc_driver_flightsql
import adbc_driver_flightsql.dbapi
import adbc_driver_manager

# Constants
GIZMOSQL_PORT = 31337
GIZMOSQL_USERNAME = "adbc_test_user"
GIZMOSQL_PASSWORD = "adbc_test_password"


def wait_for_container_log(container, timeout=30, poll_interval=1, ready_message="GizmoSQL server - started"):
    """Wait for a specific log message indicating the container is ready."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        logs = container.logs().decode("utf-8")
        if ready_message in logs:
            return True
        time.sleep(poll_interval)

    raise TimeoutError(f"Container did not show '{ready_message}' in logs within {timeout} seconds.")


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
def gizmosql_server(request):
    """Start a GizmoSQL Docker container for testing."""
    client = docker.from_env()
    container = client.containers.run(
        image="gizmodata/gizmosql:latest-slim",
        name="adbc-gizmosql-test",
        detach=True,
        remove=True,
        tty=True,
        init=True,
        ports={f"{GIZMOSQL_PORT}/tcp": GIZMOSQL_PORT},
        environment={
            "GIZMOSQL_USERNAME": GIZMOSQL_USERNAME,
            "GIZMOSQL_PASSWORD": GIZMOSQL_PASSWORD,
            "TLS_ENABLED": "0",
            "PRINT_QUERIES": "1",
            "DATABASE_FILENAME": "adbc_test.db",
            "GIZMOSQL_LOG_LEVEL": "DEBUG",
        },
        stdout=True,
        stderr=True,
    )

    def print_logs_and_stop():
        """Print container logs and stop the container."""
        try:
            logs = container.logs().decode("utf-8")
            print("\n" + "=" * 60)
            print("GizmoSQL Container Logs:")
            print("=" * 60)
            print(logs)
            print("=" * 60 + "\n")
        except Exception as e:
            print(f"Failed to retrieve container logs: {e}")
        finally:
            try:
                container.stop()
            except Exception as e:
                print(f"Failed to stop container: {e}")

    # Register finalizer to print logs on teardown (success or failure)
    request.addfinalizer(print_logs_and_stop)

    # Wait for the container to be ready
    try:
        wait_for_container_log(container)
    except TimeoutError:
        # Print logs if container failed to start
        logs = container.logs().decode("utf-8")
        print("\n" + "=" * 60)
        print("GizmoSQL Container Logs (startup failed):")
        print("=" * 60)
        print(logs)
        print("=" * 60 + "\n")
        raise

    yield container

