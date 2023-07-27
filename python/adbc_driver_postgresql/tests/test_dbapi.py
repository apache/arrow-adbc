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

from typing import Generator

import pyarrow
import pytest

from adbc_driver_postgresql import StatementOptions, dbapi


@pytest.fixture
def postgres(postgres_uri: str) -> Generator[dbapi.Connection, None, None]:
    with dbapi.connect(postgres_uri) as conn:
        yield conn


def test_conn_current_catalog(postgres: dbapi.Connection) -> None:
    assert postgres.adbc_current_catalog != ""


def test_conn_current_db_schema(postgres: dbapi.Connection) -> None:
    assert postgres.adbc_current_db_schema == "public"


def test_conn_change_db_schema(postgres: dbapi.Connection) -> None:
    assert postgres.adbc_current_db_schema == "public"

    with postgres.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS dbapischema")

    assert postgres.adbc_current_db_schema == "public"
    postgres.adbc_current_db_schema = "dbapischema"
    assert postgres.adbc_current_db_schema == "dbapischema"


def test_conn_get_info(postgres: dbapi.Connection) -> None:
    info = postgres.adbc_get_info()
    assert info["driver_name"] == "ADBC PostgreSQL Driver"
    assert info["driver_adbc_version"] == 1_001_000
    assert info["vendor_name"] == "PostgreSQL"


def test_query_batch_size(postgres: dbapi.Connection):
    with postgres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_batch_size")
        cur.execute("CREATE TABLE test_batch_size (ints INT)")
        cur.execute(
            """
            INSERT INTO test_batch_size (ints)
            SELECT generated :: INT
            FROM GENERATE_SERIES(1, 65536) temp(generated)
        """
        )

        cur.execute("SELECT * FROM test_batch_size")
        table = cur.fetch_arrow_table()
        assert len(table.to_batches()) == 1

        cur.adbc_statement.set_options(
            **{StatementOptions.BATCH_SIZE_HINT_BYTES.value: "1"}
        )
        assert (
            cur.adbc_statement.get_option_int(
                StatementOptions.BATCH_SIZE_HINT_BYTES.value
            )
            == 1
        )
        cur.execute("SELECT * FROM test_batch_size")
        table = cur.fetch_arrow_table()
        assert len(table.to_batches()) == 65536

        cur.adbc_statement.set_options(
            **{StatementOptions.BATCH_SIZE_HINT_BYTES.value: "4096"}
        )
        assert (
            cur.adbc_statement.get_option_int(
                StatementOptions.BATCH_SIZE_HINT_BYTES.value
            )
            == 4096
        )
        cur.execute("SELECT * FROM test_batch_size")
        table = cur.fetch_arrow_table()
        assert 64 <= len(table.to_batches()) <= 256


def test_query_cancel(postgres: dbapi.Connection) -> None:
    with postgres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_batch_size")
        cur.execute("CREATE TABLE test_batch_size (ints INT)")
        cur.execute(
            """
            INSERT INTO test_batch_size (ints)
            SELECT generated :: INT
            FROM GENERATE_SERIES(1, 65536) temp(generated)
        """
        )

        cur.execute("SELECT * FROM test_batch_size")
        cur.adbc_cancel()
        # XXX(https://github.com/apache/arrow-adbc/issues/940):
        # PyArrow swallows the errno and doesn't set it into the
        # OSError, so we have no clue what happened here. (Though the
        # driver does properly return ECANCELED.)
        with pytest.raises(OSError, match="canceling statement"):
            cur.fetchone()

        num_details = cur.adbc_statement.get_option_int("adbc.error_details")
        assert num_details > 0

        details = {
            cur.adbc_statement.get_option(
                f"adbc.error_details.{i}"
            ): cur.adbc_statement.get_option_bytes(f"adbc.error_details.{i}")
            for i in range(num_details)
        }
        assert details["SQLSTATE"] == b"57014"


def test_query_execute_schema(postgres: dbapi.Connection) -> None:
    with postgres.cursor() as cur:
        schema = cur.adbc_execute_schema("SELECT 1 AS foo")
        assert schema == pyarrow.schema([("foo", "int32")])


def test_query_invalid(postgres: dbapi.Connection) -> None:
    with postgres.cursor() as cur:
        with pytest.raises(
            postgres.OperationalError, match="failed to prepare query"
        ) as excinfo:
            cur.execute("SELECT * FROM tabledoesnotexist")

        assert len(excinfo.value.details) > 0
        details = dict(excinfo.value.details)
        assert details["SQLSTATE"] == b"42P01"


def test_query_trivial(postgres: dbapi.Connection):
    with postgres.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_stmt_ingest(postgres: dbapi.Connection) -> None:
    pass


def test_ddl(postgres: dbapi.Connection):
    with postgres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_ddl")
        assert cur.fetchone() is None

        cur.execute("CREATE TABLE test_ddl (ints INT)")
        assert cur.fetchone() is None

        cur.execute("INSERT INTO test_ddl VALUES (1) RETURNING ints")
        assert cur.fetchone() == (1,)

        cur.execute("SELECT * FROM test_ddl")
        assert cur.fetchone() == (1,)
