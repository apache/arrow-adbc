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

import sys

import pyarrow
import pytest

from adbc_driver_manager import dbapi

try:
    import duckdb
except ImportError:
    pass

pytestmark = pytest.mark.duckdb


@pytest.fixture
def conn():
    if sys.platform.startswith("win"):
        pytest.xfail("not supported on Windows")
    with dbapi.connect(driver=duckdb.__file__, entrypoint="duckdb_adbc_init") as conn:
        yield conn


@pytest.mark.xfail
def test_connection_get_info(conn):
    assert conn.adbc_get_info() != {}


def test_connection_get_objects(conn):
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE getobjects (ints BIGINT)")
    assert conn.adbc_get_objects(depth="all").read_all().to_pylist() != []


@pytest.mark.xfail
def test_connection_get_table_schema(conn):
    with conn.cursor() as cursor:
        cursor.execute("CREATE TABLE tableschema (ints BIGINT)")
    assert conn.adbc_get_table_schema("tableschema") == pyarrow.schema(
        [
            ("ints", "int64"),
        ]
    )


def test_connection_get_table_types(conn):
    assert conn.adbc_get_table_types() == []


@pytest.mark.xfail
def test_statement_ingest(conn):
    table = pyarrow.table(
        [
            [1, 2, 3, 4],
            ["a", "b", None, "d"],
        ],
        names=["ints", "strs"],
    )
    with conn.cursor() as cursor:
        cursor.adbc_ingest("ingest", table)
        cursor.execute("SELECT * FROM ingest")
        assert cursor.fetch_arrow_table() == table


def test_statement_prepare(conn):
    with conn.cursor() as cursor:
        cursor.adbc_prepare("SELECT 1")
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None


def test_statement_query(conn):
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1")
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() is None

        cursor.execute("SELECT 1 AS foo")
        assert cursor.fetch_arrow_table().to_pylist() == [{"foo": 1}]
