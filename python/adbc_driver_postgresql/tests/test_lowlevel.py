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

import typing

import pyarrow
import pytest

import adbc_driver_manager
import adbc_driver_postgresql


@pytest.fixture
def postgres(
    postgres_uri: str,
) -> typing.Generator[adbc_driver_manager.AdbcConnection, None, None]:
    with adbc_driver_postgresql.connect(postgres_uri) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


def test_connection_get_table_schema(postgres: adbc_driver_manager.AdbcConnection):
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="NOT_FOUND"):
        postgres.get_table_schema(None, None, "thistabledoesnotexist")


def test_query_trivial(postgres: adbc_driver_manager.AdbcConnection) -> None:
    with adbc_driver_manager.AdbcStatement(postgres) as stmt:
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        with pyarrow.RecordBatchReader._import_from_c(stream.address) as reader:
            assert reader.read_all()


def test_version() -> None:
    assert adbc_driver_postgresql.__version__  # type:ignore


def test_failed_connection() -> None:
    with pytest.raises(
        adbc_driver_manager.OperationalError, match=".*libpq.*Failed to connect.*"
    ):
        adbc_driver_postgresql.connect("invalid")


@pytest.mark.parametrize("drain", [False, True])
def test_transaction(postgres_uri: str, drain: bool) -> None:
    # regression test for https://github.com/apache/arrow-adbc/issues/4695
    status = adbc_driver_postgresql.ConnectionOptions.TRANSACTION_STATUS.value
    with adbc_driver_postgresql.connect(postgres_uri) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            with adbc_driver_manager.AdbcStatement(conn) as stmt:
                stmt.set_sql_query("DROP TABLE IF EXISTS test_transaction")
                stmt.execute_update()
                stmt.set_sql_query("CREATE TABLE test_transaction (id INT)")
                stmt.execute_update()

        with adbc_driver_manager.AdbcConnection(db) as conn:
            with adbc_driver_manager.AdbcStatement(conn) as stmt:
                stmt.set_sql_query("SELECT COUNT(*) FROM test_transaction")
                handle, _ = stmt.execute_query()
                with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                    if drain:
                        result = reader.read_all()
                        assert result[0][0].as_py() == 0
            assert conn.get_option(status) == ("idle" if drain else "active")

            conn.set_autocommit(False)
            assert conn.get_option(status) == ("idle" if drain else "active")
            with adbc_driver_manager.AdbcStatement(conn) as stmt:
                stmt.set_sql_query("INSERT INTO test_transaction (id) VALUES (1)")
                stmt.execute_update()
            assert conn.get_option(status) == "intrans"
            conn.rollback()
            assert conn.get_option(status) == "idle"

        with adbc_driver_manager.AdbcConnection(db) as conn:
            with adbc_driver_manager.AdbcStatement(conn) as stmt:
                stmt.set_sql_query("SELECT COUNT(*) FROM test_transaction")
                handle, _ = stmt.execute_query()
                with pyarrow.RecordBatchReader._import_from_c(handle.address) as reader:
                    result = reader.read_all()
                assert result[0][0].as_py() == 0
