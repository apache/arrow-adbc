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

import pyarrow as pa
import pytest

from adbc_driver_sqlite import dbapi


@pytest.fixture
def sqlite():
    with dbapi.connect() as conn:
        yield conn


def test_query_trivial(sqlite) -> None:
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_autocommit(tmp_path: Path) -> None:
    # apache/arrow-adbc#599
    db = tmp_path / "tmp.sqlite"
    with dbapi.connect(f"file:{db}") as conn:
        assert not conn._autocommit
        with conn.cursor() as cur:
            with pytest.raises(
                dbapi.OperationalError,
                match="cannot change into wal mode from within a transaction",
            ):
                cur.execute("PRAGMA journal_mode = WAL")

    # This now works if we enable autocommit
    with dbapi.connect(f"file:{db}", autocommit=True) as conn:
        assert conn._autocommit
        with conn.cursor() as cur:
            cur.execute("PRAGMA journal_mode = WAL")

    # Or we can use executescript
    with dbapi.connect(f"file:{db}") as conn:
        assert not conn._autocommit
        with conn.cursor() as cur:
            cur.executescript("PRAGMA journal_mode = WAL")


def test_create_types(tmp_path: Path) -> None:
    db = tmp_path / "foo.sqlite"
    with dbapi.connect(f"file:{db}") as conn:
        tbl = pa.Table.from_pydict({"numbers": [1, 2], "letters": ["a", "b"]})

        with conn.cursor() as cur:
            cur.adbc_ingest("foo", tbl, "create")
            cur.execute("PRAGMA table_info('foo')")
            table_info = cur.fetchall()

            assert len(table_info) == len(tbl.columns)
            """
            A tuple from table_info should return 6 values:
            - cid
            - name
            - type
            - notnull
            - dflt_value
            - pk
            For details: https://www.sqlite.org/pragma.html
            """
            for col in table_info:
                assert len(col) == 6
            actual_types = [col[2] for col in table_info]
            assert actual_types == ["INTEGER", "TEXT"]


def test_ingest() -> None:
    table = pa.Table.from_pydict({"numbers": [1, 2], "letters": ["a", "b"]})

    with dbapi.connect() as conn:
        with conn.cursor() as cur:
            cur.adbc_ingest("foo", table, catalog_name="main")
            cur.adbc_ingest("foo", table, catalog_name="temp")

            cur.execute("SELECT * FROM main.foo")
            assert cur.fetch_arrow_table() == table

            cur.execute("SELECT * FROM temp.foo")
            assert cur.fetch_arrow_table() == table

            with pytest.raises(dbapi.NotSupportedError):
                cur.adbc_ingest("foo", table, db_schema_name="main")


def test_extension() -> None:
    with dbapi.connect() as conn:
        # Can't load extensions until we enable loading
        with pytest.raises(conn.OperationalError):
            conn.load_extension("nonexistent")

        conn.enable_load_extension(False)

        with pytest.raises(conn.OperationalError):
            conn.load_extension("nonexistent")

        conn.enable_load_extension(True)

        # We don't have a real extension to test, so these still fail
        with pytest.raises(conn.OperationalError):
            conn.load_extension("nonexistent")

        with pytest.raises(conn.OperationalError):
            conn.load_extension("nonexistent", entrypoint="entrypoint")
