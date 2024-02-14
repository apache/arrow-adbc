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

from pathlib import Path
from typing import Generator

import pyarrow
import pyarrow.dataset
import pytest

from adbc_driver_netezza import StatementOptions, dbapi


@pytest.fixture
def postgres(netezza_uri: str) -> Generator[dbapi.Connection, None, None]:
    with dbapi.connect(netezza_uri) as conn:
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
    assert info["driver_name"] == "ADBC Netezza Driver"
    assert info["driver_adbc_version"] == 1_001_000
    assert info["vendor_name"] == "Netezza"


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
            FROM GENERATE_SERIES(1, 1048576) temp(generated)
        """
        )
        postgres.commit()

    # Ensure different ways of reading all raise the desired error
    with postgres.cursor() as cur:
        cur.execute("SELECT * FROM test_batch_size")
        cur.adbc_cancel()
        with pytest.raises(postgres.OperationalError, match="canceling statement"):
            cur.fetchone()

    postgres.rollback()

    with postgres.cursor() as cur:
        cur.execute("SELECT * FROM test_batch_size")
        cur.adbc_cancel()
        with pytest.raises(postgres.OperationalError, match="canceling statement"):
            cur.fetch_arrow_table()

    postgres.rollback()

    with postgres.cursor() as cur:
        cur.execute("SELECT * FROM test_batch_size")
        cur.adbc_cancel()
        with pytest.raises(postgres.OperationalError, match="canceling statement"):
            cur.fetch_df()


def test_query_execute_schema(postgres: dbapi.Connection) -> None:
    with postgres.cursor() as cur:
        schema = cur.adbc_execute_schema("SELECT 1 AS foo")
        assert schema == pyarrow.schema([("foo", "int32")])


def test_query_invalid(postgres: dbapi.Connection) -> None:
    with postgres.cursor() as cur:
        with pytest.raises(
            postgres.ProgrammingError, match="failed to prepare query"
        ) as excinfo:
            cur.execute("SELECT * FROM tabledoesnotexist")

        assert excinfo.value.sqlstate == "42P01"
        assert len(excinfo.value.details) > 0


def test_query_trivial(postgres: dbapi.Connection):
    with postgres.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_stmt_ingest(postgres: dbapi.Connection) -> None:
    table = pyarrow.table(
        [
            [1, 2, 3],
            ["a", None, "b"],
        ],
        names=["ints", "strs"],
    )
    double_table = pyarrow.table(
        [
            [1, 1, 2, 2, 3, 3],
            ["a", "a", None, None, "b", "b"],
        ],
        names=["ints", "strs"],
    )

    with postgres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_ingest")

        with pytest.raises(
            postgres.ProgrammingError, match='"public.test_ingest" does not exist'
        ):
            cur.adbc_ingest("test_ingest", table, mode="append")
        postgres.rollback()

        cur.adbc_ingest("test_ingest", table, mode="replace")
        cur.execute("SELECT * FROM test_ingest ORDER BY ints")
        assert cur.fetch_arrow_table() == table

        with pytest.raises(
            postgres.ProgrammingError, match='"test_ingest" already exists'
        ):
            cur.adbc_ingest("test_ingest", table, mode="create")

        cur.adbc_ingest("test_ingest", table, mode="create_append")
        cur.execute("SELECT * FROM test_ingest ORDER BY ints")
        assert cur.fetch_arrow_table() == double_table

        cur.adbc_ingest("test_ingest", table, mode="replace")
        cur.execute("SELECT * FROM test_ingest ORDER BY ints")
        assert cur.fetch_arrow_table() == table

        cur.execute("DROP TABLE IF EXISTS test_ingest")

        cur.adbc_ingest("test_ingest", table, mode="create_append")
        cur.execute("SELECT * FROM test_ingest ORDER BY ints")
        assert cur.fetch_arrow_table() == table

        cur.execute("DROP TABLE IF EXISTS test_ingest")

        cur.adbc_ingest("test_ingest", table, mode="create")
        cur.execute("SELECT * FROM test_ingest ORDER BY ints")
        assert cur.fetch_arrow_table() == table


def test_stmt_ingest_dataset(postgres: dbapi.Connection, tmp_path: Path) -> None:
    # Regression test for https://github.com/apache/arrow-adbc/issues/1310
    table = pyarrow.table(
        [
            [1, 1, 2, 2, 3, 3],
            ["a", "a", None, None, "b", "b"],
        ],
        schema=pyarrow.schema([("ints", "int32"), ("strs", "string")]),
    )
    pyarrow.dataset.write_dataset(
        table, tmp_path, format="parquet", partitioning=["ints"]
    )
    ds = pyarrow.dataset.dataset(tmp_path, format="parquet", partitioning=["ints"])

    with postgres.cursor() as cur:
        for item in (
            lambda: ds,
            lambda: ds.scanner(),
            lambda: ds.scanner().to_reader(),
            lambda: ds.scanner().to_table(),
        ):
            cur.execute("DROP TABLE IF EXISTS test_ingest")

            cur.adbc_ingest(
                "test_ingest",
                item(),
                mode="create_append",
            )
            cur.execute("SELECT ints, strs FROM test_ingest ORDER BY ints")
            assert cur.fetch_arrow_table() == table


def test_stmt_ingest_multi(postgres: dbapi.Connection) -> None:
    # Regression test for https://github.com/apache/arrow-adbc/issues/1310
    table = pyarrow.table(
        [
            [1, 1, 2, 2, 3, 3],
            ["a", "a", None, None, "b", "b"],
        ],
        names=["ints", "strs"],
    )

    with postgres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_ingest")

        cur.adbc_ingest(
            "test_ingest",
            table.to_batches(max_chunksize=2),
            mode="create_append",
        )
        cur.execute("SELECT * FROM test_ingest ORDER BY ints")
        assert cur.fetch_arrow_table() == table


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


def test_crash(postgres: dbapi.Connection) -> None:
    with postgres.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_reuse(postgres: dbapi.Connection) -> None:
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

        cur.execute("SELECT * FROM test_batch_size ORDER BY ints ASC")
        assert cur.fetchone() == (1,)

        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)

        cur.execute("SELECT 2")
        assert cur.fetchone() == (2,)


def test_ingest(postgres: dbapi.Connection) -> None:
    table = pyarrow.Table.from_pydict({"numbers": [1, 2], "letters": ["a", "b"]})

    with postgres.cursor() as cur:
        cur.adbc_ingest("foo", table, mode="replace", db_schema_name="public")

        cur.execute("SELECT * FROM public.foo")
        assert cur.fetch_arrow_table() == table

        with pytest.raises(dbapi.NotSupportedError):
            cur.adbc_ingest("foo", table, catalog_name="main")


def test_ingest_schema(postgres: dbapi.Connection) -> None:
    table = pyarrow.Table.from_pydict({"numbers": [1, 2], "letters": ["a", "b"]})

    with postgres.cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS testschema")
        cur.execute("DROP TABLE IF EXISTS testschema.foo")

        postgres.commit()

        cur.adbc_ingest("foo", table, mode="create", db_schema_name="testschema")

        cur.execute("SELECT * FROM testschema.foo ORDER BY numbers")
        assert cur.fetch_arrow_table() == table


def test_ingest_temporary(postgres: dbapi.Connection) -> None:
    table = pyarrow.Table.from_pydict(
        {
            "numbers": [1, 2],
            "letters": ["a", "b"],
        }
    )
    temp = pyarrow.Table.from_pydict(
        {
            "ints": [3, 4],
            "strs": ["c", "d"],
        }
    )

    table2 = pyarrow.Table.from_pydict(
        {
            "numbers": [1, 2, 1, 2],
            "letters": ["a", "b", "a", "b"],
        }
    )
    temp2 = pyarrow.Table.from_pydict(
        {
            "ints": [3, 4, 3, 4],
            "strs": ["c", "d", "c", "d"],
        }
    )

    with postgres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS public.temporary")
        cur.execute("DROP TABLE IF EXISTS pg_temp.temporary")

        cur.adbc_ingest("temporary", table, mode="create")
        cur.adbc_ingest("temporary", temp, mode="create", temporary=True)

        cur.execute("SELECT * FROM public.temporary")
        assert cur.fetch_arrow_table() == table
        cur.execute("SELECT * FROM pg_temp.temporary")
        assert cur.fetch_arrow_table() == temp
        cur.execute("SELECT * FROM temporary")
        assert cur.fetch_arrow_table() == temp

        cur.adbc_ingest("temporary", table, mode="append")
        cur.adbc_ingest("temporary", temp, mode="append", temporary=True)

        cur.execute("SELECT * FROM public.temporary")
        assert cur.fetch_arrow_table() == table2
        cur.execute("SELECT * FROM pg_temp.temporary")
        assert cur.fetch_arrow_table() == temp2
        cur.execute("SELECT * FROM temporary")
        assert cur.fetch_arrow_table() == temp2

        cur.adbc_ingest("temporary", table, mode="replace")
        cur.adbc_ingest("temporary", temp, mode="replace", temporary=True)

        cur.execute("SELECT * FROM public.temporary")
        assert cur.fetch_arrow_table() == table
        cur.execute("SELECT * FROM pg_temp.temporary")
        assert cur.fetch_arrow_table() == temp
        cur.execute("SELECT * FROM temporary")
        assert cur.fetch_arrow_table() == temp

        cur.adbc_ingest("temporary", table, mode="create_append")
        cur.adbc_ingest("temporary", temp, mode="create_append", temporary=True)

        cur.execute("SELECT * FROM public.temporary")
        assert cur.fetch_arrow_table() == table2
        cur.execute("SELECT * FROM pg_temp.temporary")
        assert cur.fetch_arrow_table() == temp2
        cur.execute("SELECT * FROM temporary")
        assert cur.fetch_arrow_table() == temp2
