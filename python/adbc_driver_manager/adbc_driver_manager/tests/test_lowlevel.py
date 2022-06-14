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

import adbc_driver_manager._lib as _lib
import pyarrow
import pytest

# TODO: make this parameterizable on different drivers?


def test_database_init():
    with pytest.raises(
        _lib.ProgrammingError, match=".*Must provide 'driver' parameter.*"
    ):
        with _lib.AdbcDatabase():
            pass


@pytest.fixture
def sqlite():
    with _lib.AdbcDatabase(
        driver="adbc_driver_sqlite",
        entrypoint="AdbcSqliteDriverInit",
    ) as db:
        with _lib.AdbcConnection(db) as conn:
            yield (db, conn)


def test_query(sqlite):
    _, conn = sqlite
    with _lib.AdbcStatement(conn) as stmt:
        stmt.set_sql_query("SELECT 1")
        stmt.execute()
        assert stmt.get_stream().read_all() == pyarrow.table([[1]], names=["1"])


def test_prepared(sqlite):
    _, conn = sqlite
    with _lib.AdbcStatement(conn) as stmt:
        stmt.set_sql_query("SELECT ?")
        stmt.prepare()

        stmt.bind(pyarrow.table([[1, 2, 3, 4]], names=["1"]))
        stmt.execute()
        assert stmt.get_stream().read_all() == pyarrow.table(
            [[1, 2, 3, 4]], names=["?"]
        )


def test_ingest(sqlite):
    _, conn = sqlite
    data = pyarrow.table(
        [
            [1, 2, 3, 4],
            ["a", "b", "c", "d"],
        ],
        names=["ints", "strs"],
    )
    with _lib.AdbcStatement(conn) as stmt:
        stmt.set_options(**{_lib.INGEST_OPTION_TARGET_TABLE: "foo"})
        stmt.bind(data)
        stmt.execute()

        stmt.set_sql_query("SELECT * FROM foo")
        stmt.execute()
        assert stmt.get_stream().read_all() == data
