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
import typing

import pytest

from adbc_driver_manager import dbapi

polars = pytest.importorskip("polars")
polars.testing = pytest.importorskip("polars.testing")

pytestmark = pytest.mark.pyarrowless


@pytest.fixture(scope="module", autouse=True)
def no_pyarrow() -> None:
    no_skip = os.environ.get("ADBC_NO_SKIP_TESTS", "").lower() in {"1", "true", "yes"}
    try:
        import pyarrow  # noqa:F401
    except ImportError:
        return
    else:
        assert not no_skip, "pyarrow is installed, but ADBC_NO_SKIP_TESTS is set"
        pytest.skip("Skipping because pyarrow is installed")


@pytest.mark.parametrize(
    "data",
    [
        pytest.param(polars.DataFrame({"theresult": [1]}), id="polars.DataFrame"),
        pytest.param(polars.Series([{"theresult": 1}]), id="polars.Series"),
        pytest.param(
            polars.DataFrame({"theresult": [1]}).__arrow_c_stream__(),
            id="PyCapsule_Stream",
        ),
    ],
)
def test_ingest(sqlite: dbapi.Connection, data: typing.Any) -> None:
    with sqlite.cursor() as cursor:
        cursor.adbc_ingest("mytable", data)
        cursor.execute("SELECT * FROM mytable")
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "theresult": [1],
                }
            ),
        )


def test_get_objects(sqlite: dbapi.Connection) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS test_table")
        cursor.execute("CREATE TABLE test_table (theresult INT)")

    result = sqlite.adbc_get_objects()
    tables = [
        table["table_name"]
        for table in result["catalog_db_schemas"][0][0]["db_schema_tables"]
    ]
    assert "test_table" in tables


def test_query(sqlite: dbapi.Connection) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("SELECT 1 AS theresult")
        capsule = cursor.fetch_arrow()
        df = typing.cast(polars.DataFrame, polars.from_arrow(capsule))
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "theresult": [1],
                }
            ),
        )

        cursor.execute("SELECT 1 AS theresult")
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "theresult": [1],
                }
            ),
        )


def test_query_executemany(sqlite: dbapi.Connection) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS test_table")
        cursor.execute("CREATE TABLE test_table (theresult INT)")
        cursor.executemany("INSERT INTO test_table VALUES (1)", None)
        cursor.execute("SELECT * FROM test_table")
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "theresult": [1],
                }
            ),
        )


@pytest.mark.parametrize(
    "parameters",
    [
        [[1, "a"], [2, "b"], [4, "c"], [8, "d"]],
        polars.DataFrame({"theresult": [1, 2, 4, 8], "strs": ["a", "b", "c", "d"]}),
    ],
)
def test_query_executemany_parameters(sqlite: dbapi.Connection, parameters) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("DROP TABLE IF EXISTS test_table")
        cursor.execute("CREATE TABLE test_table (theresult INT, strs STRING)")
        cursor.executemany("INSERT INTO test_table VALUES (?, ?)", parameters)
        cursor.execute("SELECT * FROM test_table ORDER BY theresult")
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "theresult": [1, 2, 4, 8],
                    "strs": ["a", "b", "c", "d"],
                }
            ),
        )


def test_execute_parameters_name(sqlite):
    with sqlite.cursor() as cursor:
        cursor.execute("SELECT @a + 1, @b", {"@b": 2, "@a": 1})
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "@a + 1": [2],
                    "@b": [2],
                }
            ),
        )

        # Ensure the state of the cursor isn't affected
        cursor.execute("SELECT ?2 + 1, ?1", [2, 1])
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "?2 + 1": [2],
                    "?1": [2],
                }
            ),
        )

        cursor.execute("SELECT @a + 1, @b + @b", {"@b": 2, "@a": 1})
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "@a + 1": [2],
                    "@b + @b": [4],
                }
            ),
        )

        data = polars.DataFrame({"float": [1.0], "int": [2]})
        cursor.adbc_ingest("ingest_tester", data)
        cursor.execute("SELECT * FROM ingest_tester")
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "float": [1.0],
                    "int": [2],
                }
            ),
        )


def test_executemany_parameters_name(sqlite):
    with sqlite.cursor() as cursor:
        cursor.execute("CREATE TABLE executemany_params (a, b)")

        cursor.executemany(
            "INSERT INTO executemany_params VALUES (@a, @b)",
            [{"@b": 2, "@a": 1}, {"@b": 3, "@a": 2}],
        )
        cursor.executemany(
            "INSERT INTO executemany_params VALUES (?, ?)", [(3, 4), (4, 5)]
        )

        cursor.execute("SELECT * FROM executemany_params ORDER BY a ASC")
        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "a": [1, 2, 3, 4],
                    "b": [2, 3, 4, 5],
                }
            ),
        )


@pytest.mark.parametrize(
    "parameters",
    [
        pytest.param([1], id="pylist"),
        pytest.param((1,), id="pytuple"),
        pytest.param(polars.DataFrame({"$0": [1]}), id="polars.DataFrame"),
        pytest.param(polars.Series([{"$0": 1}]), id="polars.Series"),
        pytest.param(
            polars.DataFrame({"$0": [1]}).__arrow_c_stream__(), id="PyCapsule_Stream"
        ),
    ],
)
def test_query_bind(sqlite: dbapi.Connection, parameters: typing.Any) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("SELECT 1 + ? AS theresult", parameters=parameters)

        df = cursor.fetch_polars()
        polars.testing.assert_frame_equal(
            df,
            polars.DataFrame(
                {
                    "theresult": [2],
                }
            ),
        )


def test_query_not_permitted(sqlite: dbapi.Connection) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("SELECT 1 AS theresult")

        with pytest.raises(dbapi.ProgrammingError, match="requires PyArrow"):
            cursor.fetchone()

        with pytest.raises(dbapi.ProgrammingError, match="requires PyArrow"):
            cursor.fetchall()

        with pytest.raises(dbapi.ProgrammingError, match="requires PyArrow"):
            cursor.fetchallarrow()

        with pytest.raises(dbapi.ProgrammingError, match="requires PyArrow"):
            cursor.fetch_arrow_table()

        with pytest.raises(dbapi.ProgrammingError, match="requires PyArrow"):
            cursor.fetch_df()

        capsule = cursor.fetch_arrow()
        # Import the result to free memory
        polars.from_arrow(capsule)


def test_query_double_capsule(sqlite: dbapi.Connection) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("SELECT 1 AS theresult")

        capsule = cursor.fetch_arrow()

        with pytest.raises(dbapi.ProgrammingError, match="has been closed"):
            cursor.fetch_arrow()

        # Import the result to free memory
        polars.from_arrow(capsule)


@pytest.mark.xfail(raises=dbapi.NotSupportedError)
def test_get_table_schema(sqlite: dbapi.Connection) -> None:
    with sqlite.cursor() as cursor:
        cursor.execute("CREATE TABLE test_table_schema (a INT, b STRING)")

    schema = sqlite.adbc_get_table_schema("test_table_schema")
    assert schema == polars.Schema(
        [
            ("a", polars.Int32),
            ("b", polars.String),
        ]
    )
