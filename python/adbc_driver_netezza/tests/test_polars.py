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

# pyright: reportUnboundVariable=false
# pyright doesn't like the optional import

import uuid

import pytest

from adbc_driver_netezza import dbapi

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
        "floats",
    ],
    indirect=True,
)
def test_polars_write_database(netezza_uri: str, df: "polars.DataFrame") -> None:
    table_name = f"polars_test_ingest_{uuid.uuid4().hex}"
    try:
        df.write_database(
            table_name=table_name,
            connection=netezza_uri,
            # TODO(apache/arrow-adbc#541): polars doesn't map the semantics
            # properly here, and one of their modes isn't supported
            if_table_exists="replace",
            engine="adbc",
        )
    finally:
        with dbapi.connect(netezza_uri) as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"DROP TABLE {table_name}")
