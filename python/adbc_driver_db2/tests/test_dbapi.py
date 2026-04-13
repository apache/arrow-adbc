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

import pytest

from adbc_driver_db2 import dbapi


@pytest.fixture
def db2(db2_uri):
    with dbapi.connect(db2_uri) as conn:
        yield conn


def test_query_trivial(db2):
    with db2.cursor() as cur:
        cur.execute("SELECT 1 AS val FROM SYSIBM.SYSDUMMY1")
        row = cur.fetchone()
        assert row is not None
        assert row[0] == 1


def test_user_style_select_like_application(db2):
    """End-user flow: connect, run SELECT, consume rows (DB2 requires a FROM clause)."""
    with db2.cursor() as cur:
        cur.execute(
            """
            SELECT
              CAST(42 AS INTEGER) AS answer,
              CAST('hello' AS VARCHAR(20)) AS greeting,
              CURRENT DATE AS today
            FROM SYSIBM.SYSDUMMY1
            """
        )
        rows = cur.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == 42
        assert rows[0][1] == "hello"
        assert rows[0][2] is not None

    with db2.cursor() as cur:
        cur.execute(
            """
            SELECT id, name
            FROM (VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')) AS t(id, name)
            """
        )
        table = cur.fetch_arrow_table()
        assert table.num_rows == 3
        assert table.column("ID").to_pylist() == [1, 2, 3]
        assert table.column("NAME").to_pylist() == ["alice", "bob", "carol"]


def test_query_fetchall(db2):
    with db2.cursor() as cur:
        cur.execute(
            "SELECT col FROM (VALUES 1, 2, 3) AS t(col)"
        )
        rows = cur.fetchall()
        assert len(rows) == 3


def test_query_fetchmany(db2):
    with db2.cursor() as cur:
        cur.execute(
            "SELECT col FROM (VALUES 1, 2, 3, 4, 5) AS t(col)"
        )
        batch = cur.fetchmany(2)
        assert len(batch) == 2
        rest = cur.fetchall()
        assert len(rest) == 3


def test_fetch_arrow_table(db2):
    with db2.cursor() as cur:
        cur.execute(
            "SELECT "
            "  CAST(42 AS INTEGER) AS int_val, "
            "  CAST('hello' AS VARCHAR(50)) AS str_val "
            "FROM SYSIBM.SYSDUMMY1"
        )
        table = cur.fetch_arrow_table()
        assert table.num_rows == 1
        assert table.num_columns == 2
        assert table.column("INT_VAL").to_pylist() == [42]
        assert table.column("STR_VAL").to_pylist() == ["hello"]


def test_executemany(db2):
    with db2.cursor() as cur:
        cur.execute(
            "CREATE TABLE adbc_db2_test_executemany "
            "(id INTEGER, name VARCHAR(50))"
        )
    try:
        with db2.cursor() as cur:
            cur.execute(
                "INSERT INTO adbc_db2_test_executemany VALUES (1, 'alice')"
            )
            cur.execute(
                "INSERT INTO adbc_db2_test_executemany VALUES (2, 'bob')"
            )
        db2.commit()

        with db2.cursor() as cur:
            cur.execute("SELECT * FROM adbc_db2_test_executemany ORDER BY id")
            rows = cur.fetchall()
            assert len(rows) == 2
            assert rows[0][1] == "alice"
            assert rows[1][1] == "bob"
    finally:
        with db2.cursor() as cur:
            cur.execute("DROP TABLE adbc_db2_test_executemany")
        db2.commit()


def test_autocommit_default(db2):
    assert not db2._autocommit
    db2.commit()
    db2.rollback()


def test_autocommit_explicit(db2_uri):
    with dbapi.connect(db2_uri, autocommit=True) as conn:
        assert conn._autocommit


def test_conn_kwargs(db2_uri):
    with dbapi.connect(db2_uri, conn_kwargs={}) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM SYSIBM.SYSDUMMY1")
            assert cur.fetchone() is not None


def test_ingest_dbapi(db2):
    import pyarrow

    table = pyarrow.table(
        {
            "x": pyarrow.array([10, 20, 30], type=pyarrow.int64()),
            "y": pyarrow.array(["one", "two", "three"], type=pyarrow.string()),
        }
    )

    with db2.cursor() as cur:
        try:
            cur.execute('DROP TABLE "adbc_dbapi_ingest"')
        except Exception:
            pass

    with db2.cursor() as cur:
        cur.adbc_ingest("adbc_dbapi_ingest", table, mode="create")
    db2.commit()

    try:
        with db2.cursor() as cur:
            cur.execute('SELECT * FROM "adbc_dbapi_ingest" ORDER BY "x"')
            rows = cur.fetchall()
            assert len(rows) == 3
            assert rows[0][0] == 10
            assert rows[2][1] == "three"
    finally:
        with db2.cursor() as cur:
            cur.execute('DROP TABLE "adbc_dbapi_ingest"')
        db2.commit()


def test_error_bad_sql(db2):
    with db2.cursor() as cur:
        with pytest.raises(dbapi.ProgrammingError):
            cur.execute("THIS IS NOT VALID SQL")


def test_description(db2):
    with db2.cursor() as cur:
        cur.execute(
            "SELECT CAST(1 AS INTEGER) AS col1, "
            "CAST('x' AS VARCHAR(10)) AS col2 "
            "FROM SYSIBM.SYSDUMMY1"
        )
        assert cur.description is not None
        assert len(cur.description) == 2
        assert cur.description[0][0].upper() == "COL1"
        assert cur.description[1][0].upper() == "COL2"


def test_failed_connection():
    with pytest.raises(Exception):
        dbapi.connect(
            "DATABASE=nonexistent;HOSTNAME=192.0.2.1;PORT=99999;"
            "PROTOCOL=TCPIP;UID=bad;PWD=bad"
        )
