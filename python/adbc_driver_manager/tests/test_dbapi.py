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

import pathlib

import pandas
import pyarrow
import pyarrow.dataset
import pytest
from pandas.testing import assert_frame_equal

from adbc_driver_manager import dbapi


def test_type_objects():
    assert dbapi.NUMBER == pyarrow.int64()
    assert pyarrow.int64() == dbapi.NUMBER

    assert dbapi.STRING == pyarrow.string()
    assert pyarrow.string() == dbapi.STRING

    assert dbapi.STRING != dbapi.NUMBER
    assert dbapi.NUMBER != dbapi.DATETIME
    assert dbapi.NUMBER == dbapi.ROWID


@pytest.mark.sqlite
def test_attrs(sqlite):
    assert sqlite.Warning == dbapi.Warning
    assert sqlite.Error == dbapi.Error
    assert sqlite.InterfaceError == dbapi.InterfaceError
    assert sqlite.DatabaseError == dbapi.DatabaseError
    assert sqlite.DataError == dbapi.DataError
    assert sqlite.OperationalError == dbapi.OperationalError
    assert sqlite.IntegrityError == dbapi.IntegrityError
    assert sqlite.InternalError == dbapi.InternalError
    assert sqlite.ProgrammingError == dbapi.ProgrammingError
    assert sqlite.NotSupportedError == dbapi.NotSupportedError

    with sqlite.cursor() as cur:
        assert cur.arraysize == 1
        assert cur.connection is sqlite
        assert cur.description is None
        assert cur.rowcount == -1


@pytest.mark.sqlite
def test_info(sqlite):
    info = sqlite.adbc_get_info()
    assert set(info.keys()) == {
        "driver_arrow_version",
        "driver_name",
        "driver_version",
        "vendor_name",
        "vendor_version",
    }
    assert info["driver_name"] == "ADBC SQLite Driver"
    assert info["vendor_name"] == "SQLite"


@pytest.mark.sqlite
def test_get_underlying(sqlite):
    assert sqlite.adbc_database
    assert sqlite.adbc_connection
    with sqlite.cursor() as cur:
        assert cur.adbc_statement


@pytest.mark.sqlite
def test_clone(sqlite):
    with sqlite.adbc_clone() as sqlite2:
        with sqlite2.cursor() as cur:
            cur.execute("CREATE TABLE temporary (ints)")
            cur.execute("INSERT INTO temporary VALUES (1)")
        sqlite2.commit()

    with sqlite.cursor() as cur:
        cur.execute("SELECT * FROM temporary")
        assert cur.fetchone() == (1,)


@pytest.mark.sqlite
def test_get_objects(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("CREATE TABLE temporary (ints)")
        cur.execute("INSERT INTO temporary VALUES (1)")
    metadata = (
        sqlite.adbc_get_objects(table_name_filter="temporary").read_all().to_pylist()
    )
    assert len(metadata) == 1
    assert metadata[0]["catalog_name"] == "main"
    schemas = metadata[0]["catalog_db_schemas"]
    assert len(schemas) == 1
    assert schemas[0]["db_schema_name"] == ""
    tables = schemas[0]["db_schema_tables"]
    assert len(tables) == 1
    assert tables[0]["table_name"] == "temporary"
    assert tables[0]["table_type"] == "table"
    assert tables[0]["table_columns"][0]["column_name"] == "ints"
    assert tables[0]["table_columns"][0]["ordinal_position"] == 1
    assert tables[0]["table_constraints"] == []


@pytest.mark.sqlite
def test_get_table_schema(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("CREATE TABLE temporary (ints)")
        cur.execute("INSERT INTO temporary VALUES (1)")
    assert sqlite.adbc_get_table_schema("temporary") == pyarrow.schema(
        [("ints", pyarrow.int64())]
    )


@pytest.mark.sqlite
def test_get_table_types(sqlite):
    assert sqlite.adbc_get_table_types() == ["table", "view"]


class ArrayWrapper:
    def __init__(self, array):
        self.array = array

    def __arrow_c_array__(self, requested_schema=None):
        return self.array.__arrow_c_array__(requested_schema=requested_schema)


class StreamWrapper:
    def __init__(self, stream):
        self.stream = stream

    def __arrow_c_stream__(self, requested_schema=None):
        return self.stream.__arrow_c_stream__(requested_schema=requested_schema)


@pytest.mark.parametrize(
    "data",
    [
        lambda: pyarrow.record_batch([[1, 2], ["foo", ""]], names=["ints", "strs"]),
        lambda: pyarrow.table([[1, 2], ["foo", ""]], names=["ints", "strs"]),
        lambda: pyarrow.table(
            [[1, 2], ["foo", ""]], names=["ints", "strs"]
        ).to_reader(),
        lambda: ArrayWrapper(
            pyarrow.record_batch([[1, 2], ["foo", ""]], names=["ints", "strs"])
        ),
        lambda: StreamWrapper(
            pyarrow.table([[1, 2], ["foo", ""]], names=["ints", "strs"])
        ),
        lambda: pyarrow.table(
            [[1, 2], ["foo", ""]], names=["ints", "strs"]
        ).__arrow_c_stream__(),
    ],
)
@pytest.mark.sqlite
def test_ingest(data, sqlite):
    with sqlite.cursor() as cur:
        cur.adbc_ingest("bulk_ingest", data())

        with pytest.raises(dbapi.Error):
            cur.adbc_ingest("bulk_ingest", data())

        cur.adbc_ingest("bulk_ingest", data(), mode="append")

        with pytest.raises(dbapi.Error):
            cur.adbc_ingest("nonexistent", data(), mode="append")

        with pytest.raises(ValueError):
            cur.adbc_ingest("bulk_ingest", data(), mode="invalid")

    with sqlite.cursor() as cur:
        cur.execute("SELECT * FROM bulk_ingest")
        assert cur.fetchone() == (1, "foo")
        assert cur.fetchone() == (2, "")
        assert cur.fetchone() == (1, "foo")
        assert cur.fetchone() == (2, "")


@pytest.mark.sqlite
def test_partitions(sqlite):
    with pytest.raises(dbapi.NotSupportedError):
        with sqlite.cursor() as cur:
            cur.adbc_execute_partitions("SELECT 1")


@pytest.mark.sqlite
def test_query_fetch_py(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1, 'foo' AS foo, 2.0")
        assert cur.description == [
            ("1", dbapi.NUMBER, None, None, None, None, None),
            ("foo", dbapi.STRING, None, None, None, None, None),
            ("2.0", dbapi.NUMBER, None, None, None, None, None),
        ]
        assert cur.rownumber == 0
        assert cur.fetchone() == (1, "foo", 2.0)
        assert cur.rownumber == 1
        assert cur.fetchone() is None

        cur.execute("SELECT 1, 'foo', 2.0")
        assert cur.fetchmany() == [(1, "foo", 2.0)]
        assert cur.fetchmany() == []

        cur.execute("SELECT 1, 'foo', 2.0")
        assert cur.fetchall() == [(1, "foo", 2.0)]
        assert cur.fetchall() == []

        cur.execute("SELECT 1, 'foo', 2.0")
        assert list(cur) == [(1, "foo", 2.0)]


@pytest.mark.sqlite
def test_query_fetch_arrow(sqlite):
    with sqlite.cursor() as cur:
        with pytest.raises(sqlite.ProgrammingError):
            cur.fetch_arrow()

        cur.execute("SELECT 1, 'foo' AS foo, 2.0")
        capsule = cur.fetch_arrow().__arrow_c_stream__()
        reader = pyarrow.RecordBatchReader._import_from_c_capsule(capsule)
        assert reader.read_all() == pyarrow.table(
            {
                "1": [1],
                "foo": ["foo"],
                "2.0": [2.0],
            }
        )

        with pytest.raises(sqlite.ProgrammingError):
            cur.fetch_arrow()


@pytest.mark.sqlite
def test_query_fetch_arrow_3543(sqlite):
    # Regression test for https://github.com/apache/arrow-adbc/issues/3543
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1, 'foo' AS foo, 2.0")

        # This should not consume the result
        assert cur.description == [
            ("1", dbapi.NUMBER, None, None, None, None, None),
            ("foo", dbapi.STRING, None, None, None, None, None),
            ("2.0", dbapi.NUMBER, None, None, None, None, None),
        ]

        capsule = cur.fetch_arrow().__arrow_c_stream__()
        reader = pyarrow.RecordBatchReader._import_from_c_capsule(capsule)
        assert reader.read_all() == pyarrow.table(
            {
                "1": [1],
                "foo": ["foo"],
                "2.0": [2.0],
            }
        )


@pytest.mark.sqlite
def test_query_fetch_arrow_table(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1, 'foo' AS foo, 2.0")
        assert cur.fetch_arrow_table() == pyarrow.table(
            {
                "1": [1],
                "foo": ["foo"],
                "2.0": [2.0],
            }
        )


@pytest.mark.sqlite
def test_query_fetch_df(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1, 'foo' AS foo, 2.0")
        assert_frame_equal(
            cur.fetch_df(),
            pandas.DataFrame(
                {
                    "1": [1],
                    "foo": ["foo"],
                    "2.0": [2.0],
                }
            ),
        )


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "parameters",
    [
        (1.0, 2),
        pyarrow.record_batch([[1.0], [2]], names=["float", "int"]),
        pyarrow.table([[1.0], [2]], names=["float", "int"]),
        ArrayWrapper(pyarrow.record_batch([[1.0], [2]], names=["float", "int"])),
        StreamWrapper(pyarrow.table([[1.0], [2]], names=["float", "int"])),
    ],
)
def test_execute_parameters(sqlite, parameters):
    with sqlite.cursor() as cur:
        cur.execute("SELECT ? + 1, ?", parameters)
        assert cur.fetchall() == [(2.0, 2)]


@pytest.mark.sqlite
def test_execute_parameters_name(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("SELECT @a + 1, @b", {"@b": 2, "@a": 1})
        assert cur.fetchall() == [(2, 2)]

        # Ensure the state of the cursor isn't affected
        cur.execute("SELECT ?2 + 1, ?1", [2, 1])
        assert cur.fetchall() == [(2, 2)]

        cur.execute("SELECT @a + 1, @b + @b", {"@b": 2, "@a": 1})
        assert cur.fetchall() == [(2, 4)]

        data = pyarrow.record_batch([[1.0], [2]], names=["float", "int"])
        cur.adbc_ingest("ingest_tester", data)
        cur.execute("SELECT * FROM ingest_tester")
        assert cur.fetchall() == [(1.0, 2)]


@pytest.mark.sqlite
def test_executemany_parameters_name(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("CREATE TABLE executemany_params (a, b)")

        cur.executemany(
            "INSERT INTO executemany_params VALUES (@a, @b)",
            [{"@b": 2, "@a": 1}, {"@b": 3, "@a": 2}],
        )
        cur.executemany(
            "INSERT INTO executemany_params VALUES (?, ?)", [(3, 4), (4, 5)]
        )

        cur.execute("SELECT * FROM executemany_params ORDER BY a ASC")
        assert cur.fetchall() == [(1, 2), (2, 3), (3, 4), (4, 5)]


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "parameters",
    [
        [(1, "a"), (3, None)],
        pyarrow.record_batch([[1, 3], ["a", None]], names=["float", "str"]),
        pyarrow.table([[1, 3], ["a", None]], names=["float", "str"]),
        pyarrow.table([[1, 3], ["a", None]], names=["float", "str"]).to_batches()[0],
        ArrayWrapper(
            pyarrow.record_batch([[1, 3], ["a", None]], names=["float", "str"])
        ),
        StreamWrapper(pyarrow.table([[1, 3], ["a", None]], names=["float", "str"])),
        ((x, y) for x, y in ((1, "a"), (3, None))),
    ],
)
def test_executemany_parameters(sqlite, parameters):
    with sqlite.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS executemany")
        cur.execute("CREATE TABLE executemany (int, str)")
        cur.executemany("INSERT INTO executemany VALUES (? * 2, ?)", parameters)
        cur.execute("SELECT * FROM executemany ORDER BY int ASC")
        assert cur.fetchall() == [(2, "a"), (6, None)]


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "parameters",
    [
        [],
        pyarrow.record_batch([[]], schema=pyarrow.schema([("v", pyarrow.int64())])),
        pyarrow.table([[]], schema=pyarrow.schema([("v", pyarrow.int64())])),
    ],
)
def test_executemany_empty(sqlite, parameters):
    # Regression test for https://github.com/apache/arrow-adbc/issues/3319
    with sqlite.cursor() as cur:
        # With an empty sequence, it should be the same as not executing the
        # query at all.
        cur.execute("DROP TABLE IF EXISTS executemany")
        cur.execute("CREATE TABLE executemany (v)")
        cur.executemany("INSERT INTO executemany VALUES (?)", parameters)
        cur.execute("SELECT * FROM executemany")
        assert cur.fetchall() == []


@pytest.mark.sqlite
def test_executemany_none(sqlite):
    # Regression test for https://github.com/apache/arrow-adbc/issues/3319
    with sqlite.cursor() as cur:
        # With None, it should be the same as executing the query once.
        cur.execute("DROP TABLE IF EXISTS executemany")
        cur.execute("CREATE TABLE executemany (v)")
        with pytest.raises(sqlite.Error):
            cur.executemany("INSERT INTO executemany VALUES (?)", None)


@pytest.mark.sqlite
def test_query_substrait(sqlite):
    with sqlite.cursor() as cur:
        with pytest.raises(dbapi.NotSupportedError):
            cur.execute(b"Substrait plan")


@pytest.mark.sqlite
def test_executemany(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("CREATE TABLE foo (a, b)")
        cur.executemany(
            "INSERT INTO foo VALUES (?, ?)",
            [
                (1, 2),
                (3, 4),
                (5, 6),
            ],
        )
        cur.execute("SELECT COUNT(*) FROM foo")
        assert cur.fetchone() == (3,)
        cur.execute("SELECT * FROM foo ORDER BY a ASC")
        assert cur.rownumber == 0
        assert next(cur) == (1, 2)
        assert cur.rownumber == 1
        assert next(cur) == (3, 4)
        assert cur.rownumber == 2
        assert next(cur) == (5, 6)


@pytest.mark.sqlite
def test_fetch_record_batch(sqlite):
    dataset = [
        [1, 2],
        [3, 4],
        [5, 6],
        [7, 8],
        [9, 10],
    ]
    with sqlite.cursor() as cur:
        cur.execute("CREATE TABLE foo (a, b)")
        cur.executemany(
            "INSERT INTO foo VALUES (?, ?)",
            dataset,
        )
        cur.execute("SELECT * FROM foo")
        rbr = cur.fetch_record_batch()
        assert rbr.read_pandas().values.tolist() == dataset


@pytest.mark.sqlite
def test_fetch_empty(sqlite):
    with sqlite.cursor() as cur:
        cur.execute("CREATE TABLE foo (bar)")
        cur.execute("SELECT * FROM foo")
        assert cur.fetchall() == []


@pytest.mark.sqlite
def test_reader(sqlite, tmp_path) -> None:
    # Regression test for https://github.com/apache/arrow-adbc/issues/1523
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1")
        reader = cur.fetch_record_batch()
        pyarrow.dataset.write_dataset(reader, tmp_path, format="parquet")


@pytest.mark.sqlite
def test_prepare(sqlite):
    with sqlite.cursor() as cur:
        schema = cur.adbc_prepare("SELECT 1")
        assert schema == pyarrow.schema([])

        schema = cur.adbc_prepare("SELECT 1 + ?")
        assert schema == pyarrow.schema([("0", "null")])

        cur.execute("SELECT 1 + ?", (1,))
        assert cur.fetchone() == (2,)


@pytest.mark.sqlite
def test_close_warning(sqlite):
    with pytest.warns(
        ResourceWarning,
        match=r"A adbc_driver_manager.dbapi.Cursor was not explicitly close\(\)d",
    ):
        cur = sqlite.cursor()
        del cur

    with pytest.warns(
        ResourceWarning,
        match=r"A adbc_driver_manager.dbapi.Connection was not explicitly close\(\)d",
    ):
        conn = dbapi.connect(driver="adbc_driver_sqlite")
        del conn


def _execute_schema(cursor):
    try:
        cursor.adbc_execute_schema("select 1")
    except dbapi.NotSupportedError:
        pass


@pytest.mark.sqlite
@pytest.mark.parametrize(
    "op",
    [
        pytest.param(lambda cursor: cursor.execute("SELECT 1"), id="execute"),
        pytest.param(
            lambda cursor: cursor.executemany("SELECT ?", [[1]]), id="executemany"
        ),
        pytest.param(
            lambda cursor: cursor.adbc_ingest(
                "test_release",
                pyarrow.table([[1]], names=["ints"]),
                mode="create_append",
            ),
            id="ingest",
        ),
        pytest.param(_execute_schema, id="execute_schema"),
        pytest.param(lambda cursor: cursor.adbc_prepare("select 1"), id="prepare"),
        pytest.param(
            lambda cursor: cursor.executescript("select 1"), id="executescript"
        ),
    ],
)
def test_release(sqlite, op) -> None:
    # Regression test. Ensure that subsequent operations free results of
    # earlier operations.
    with sqlite.cursor() as cur:
        cur.execute("select 1")
        # Do _not_ fetch the data so it is never imported.
        assert cur._results._handle.is_valid
        handle = cur._results._handle

        op(cur)
        if handle:
            # The original handle (if it exists) should have been released
            assert not handle.is_valid


def test_driver_path():
    with pytest.raises(
        dbapi.ProgrammingError,
        match="(dlopen|LoadLibraryExW).*failed:",
    ):
        with dbapi.connect(driver=pathlib.Path("/tmp/thisdriverdoesnotexist")):
            pass


@pytest.mark.sqlite
def test_dbapi_extensions(sqlite):
    with sqlite.execute("SELECT ?", (1,)) as cur:
        assert cur.fetchone() == (1,)
        assert cur.fetchone() is None

        assert cur.execute("SELECT 2").fetchall() == [(2,)]

    with sqlite.cursor() as cur:
        assert cur.execute("SELECT 1").fetchall() == [(1,)]
        assert cur.execute("SELECT 42").fetchall() == [(42,)]


@pytest.mark.sqlite
def test_close_connection_with_open_cursor():
    """
    Regression test for https://github.com/apache/arrow-adbc/issues/3713

    Closing a connection should automatically close any open cursors
    without raising RuntimeError about open AdbcStatement.

    This test intentionally avoids context managers because the bug only
    manifests when users manually call close() without first closing cursors.
    """
    conn = dbapi.connect(driver="adbc_driver_sqlite")
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    # Fetch the result to ensure the cursor is "active"
    cursor.fetch_arrow_table()

    # This should NOT raise:
    # RuntimeError: Cannot close AdbcConnection with open AdbcStatement
    conn.close()

    # Verify cursor is also closed
    assert cursor._closed


@pytest.mark.sqlite
def test_close_connection_with_multiple_open_cursors():
    """
    Test that closing a connection closes all open cursors.

    This test intentionally avoids context managers because the bug only
    manifests when users manually call close() without first closing cursors.
    """
    conn = dbapi.connect(driver="adbc_driver_sqlite")
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()
    cursor3 = conn.cursor()

    cursor1.execute("SELECT 1")
    cursor2.execute("SELECT 2")
    # cursor3 is not executed

    # This should close all cursors
    conn.close()

    assert cursor1._closed
    assert cursor2._closed
    assert cursor3._closed


@pytest.mark.sqlite
def test_close_connection_cursor_already_closed():
    """
    Test that closing a connection works even if some cursors are already closed.

    This test intentionally avoids context managers because the bug only
    manifests when users manually call close() without first closing cursors.
    """
    conn = dbapi.connect(driver="adbc_driver_sqlite")
    cursor1 = conn.cursor()
    cursor2 = conn.cursor()

    cursor1.execute("SELECT 1")
    cursor1.close()  # Manually close cursor1

    cursor2.execute("SELECT 2")
    # cursor2 is still open

    # This should work without issues
    conn.close()

    assert cursor1._closed
    assert cursor2._closed


@pytest.mark.sqlite
def test_close_connection_via_context_manager_with_open_cursors():
    """
    Test that exiting a connection context manager closes open cursors.

    This test uses a context manager for the connection but intentionally
    avoids context managers for cursors to verify that Connection.__exit__
    properly closes any open cursors.
    """
    with dbapi.connect(driver="adbc_driver_sqlite") as conn:
        cursor1 = conn.cursor()
        cursor2 = conn.cursor()

        cursor1.execute("SELECT 1")
        cursor2.execute("SELECT 2")

        # Cursors are open at this point
        assert not cursor1._closed
        assert not cursor2._closed

    # After exiting the context manager, cursors should be closed
    assert cursor1._closed
    assert cursor2._closed


@pytest.mark.sqlite
def test_close_connection_suppresses_cursor_close_error():
    """
    Test that closing a connection suppresses exceptions from cursor.close().

    When a cursor's close() method raises an exception, the connection should
    still close successfully without propagating the error.
    """
    from unittest.mock import MagicMock

    conn = dbapi.connect(driver="adbc_driver_sqlite")
    # Create a real cursor so we have something legitimate in _cursors
    real_cursor = conn.cursor()
    real_cursor.execute("SELECT 1")

    # Create a mock cursor that raises on close
    mock_cursor = MagicMock()
    mock_cursor.close.side_effect = RuntimeError("Simulated cursor close failure")

    # Add the mock cursor to the connection's cursor set
    conn._cursors.add(mock_cursor)

    # This should NOT raise despite the mock cursor raising on close
    conn.close()

    # Verify the connection is closed
    assert conn._closed

    # Verify the mock cursor's close was called
    mock_cursor.close.assert_called_once()

    # Verify the real cursor is also closed
    assert real_cursor._closed


@pytest.mark.sqlite
def test_connect(tmp_path: pathlib.Path, monkeypatch) -> None:
    with dbapi.connect(driver="adbc_driver_sqlite") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    # https://github.com/apache/arrow-adbc/issues/3517: allow positional
    # argument
    with dbapi.connect("adbc_driver_sqlite") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)

    # https://github.com/apache/arrow-adbc/issues/3517: allow URI argument
    db = tmp_path / "test.db"
    with dbapi.connect("adbc_driver_sqlite", db.as_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE foo (a)")
            cur.execute("INSERT INTO foo VALUES (1)")
        conn.commit()

    with dbapi.connect(driver="adbc_driver_sqlite", uri=db.as_uri()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM foo")
            assert cur.fetchone() == (1,)

    monkeypatch.setenv("ADBC_DRIVER_PATH", tmp_path)
    with (tmp_path / "foobar.toml").open("w") as f:
        f.write(
            """
[Driver]
shared = "adbc_driver_foobar"
        """
        )
    # Just check that the driver gets detected and loaded (should fail)
    with pytest.raises(dbapi.ProgrammingError, match="NOT_FOUND"):
        with dbapi.connect("foobar://localhost:5439"):
            pass
