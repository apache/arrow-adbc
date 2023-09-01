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

import pyarrow
import pytest

import adbc_driver_manager
import adbc_driver_sqlite


@pytest.fixture
def sqlite():
    with adbc_driver_sqlite.connect() as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


def test_connection_get_table_schema(sqlite):
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="NOT_FOUND"):
        sqlite.get_table_schema(None, None, "thistabledoesnotexist")


def test_query_trivial(sqlite):
    with adbc_driver_manager.AdbcStatement(sqlite) as stmt:
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all()


def test_options(sqlite):
    with adbc_driver_manager.AdbcStatement(sqlite) as stmt:
        stmt.set_options(
            **{
                adbc_driver_sqlite.StatementOptions.BATCH_ROWS.value: "1",
            }
        )
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all()


def test_version():
    assert adbc_driver_sqlite.__version__  # type:ignore
