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

import pandas
import pyarrow
import pyarrow.dataset
import pytest
from pandas.testing import assert_frame_equal

from adbc_driver_manager import dbapi


@pytest.fixture
def sqlite():
    """Dynamically load the SQLite driver."""
    with dbapi.connect(driver="adbc_driver_sqlite") as conn:
        yield conn


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
        cur.execute('SELECT 1, "foo", 2.0')
        assert cur.description == [
            ("1", dbapi.NUMBER, None, None, None, None, None),
            ('"foo"', dbapi.STRING, None, None, None, None, None),
            ("2.0", dbapi.NUMBER, None, None, None, None, None),
        ]
        assert cur.rownumber == 0
        assert cur.fetchone() == (1, "foo", 2.0)
        assert cur.rownumber == 1
        assert cur.fetchone() is None

        cur.execute('SELECT 1, "foo", 2.0')
        assert cur.fetchmany() == [(1, "foo", 2.0)]
        assert cur.fetchmany() == []

        cur.execute('SELECT 1, "foo", 2.0')
        assert cur.fetchall() == [(1, "foo", 2.0)]
        assert cur.fetchall() == []

        cur.execute('SELECT 1, "foo", 2.0')
        assert list(cur) == [(1, "foo", 2.0)]


@pytest.mark.sqlite
def test_query_fetch_arrow(sqlite):
    with sqlite.cursor() as cur:
        cur.execute('SELECT 1, "foo", 2.0')
        assert cur.fetch_arrow_table() == pyarrow.table(
            {
                "1": [1],
                '"foo"': ["foo"],
                "2.0": [2.0],
            }
        )


@pytest.mark.sqlite
def test_query_fetch_df(sqlite):
    with sqlite.cursor() as cur:
        cur.execute('SELECT 1, "foo", 2.0')
        assert_frame_equal(
            cur.fetch_df(),
            pandas.DataFrame(
                {
                    "1": [1],
                    '"foo"': ["foo"],
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
