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

"""Integration tests with polars."""

import uuid

import pytest

from adbc_driver_postgresql import dbapi

try:
    import polars
except ImportError:
    pass

pytestmark = pytest.mark.polars


@pytest.fixture
def df(request):
    if request.param == "ints":
        return polars.DataFrame(
            {
                "ints": [1, 2, 4, 8],
            },
            schema={
                "ints": polars.Int64,
            },
        )
    elif request.param == "floats":
        return polars.DataFrame(
            {
                "floats": [1, 2, 4, 8],
            },
            schema={
                "floats": polars.Float64,
            },
        )
    raise KeyError(f"Unknown df {request.param}")


@pytest.mark.parametrize(
    "df",
    [
        "ints",
        pytest.param(
            "floats",
            marks=pytest.mark.xfail(
                reason="; ".join(
                    [
                        "apache/arrow-adbc#81: lack of type support",
                        "pola-rs/polars#7757: polars doesn't close cursor properly",
                    ]
                )
            ),
        ),
    ],
    indirect=True,
)
def test_polars_write_database(postgres_uri: str, df: "polars.DataFrame") -> None:
    table_name = f"polars_test_ingest_{uuid.uuid4().hex}"
    try:
        df.write_database(
            table_name=table_name,
            connection_uri=postgres_uri,
            # TODO(apache/arrow-adbc#541): polars doesn't map the semantics
            # properly here, and one of their modes isn't supported
            if_exists="replace",
            engine="adbc",
        )
    finally:
        # TODO(apache/arrow-adbc#540): driver doesn't handle execute()
        # properly here because it tries to infer the schema.
        with dbapi.connect(postgres_uri) as conn:
            with conn.cursor() as cursor:
                cursor.executemany(f"DROP TABLE {table_name}", [])
