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

import polars
import polars.testing
import pytest

from adbc_driver_manager import dbapi

pytestmark = pytest.mark.pyarrowless


@pytest.fixture(scope="module", autouse=True)
def no_pyarrow() -> None:
    try:
        import pyarrow  # noqa:F401
    except ImportError:
        return
    else:
        pytest.skip("Skipping because pyarrow is installed")


@pytest.fixture
def sqlite() -> typing.Generator[dbapi.Connection, None, None]:
    with dbapi.connect(driver="adbc_driver_sqlite") as conn:
        yield conn


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


@pytest.mark.parametrize(
    "parameters",
    [
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
