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
import adbc_driver_snowflake


@pytest.fixture
def snowflake(snowflake_uri: str):
    with adbc_driver_snowflake.connect(snowflake_uri) as db:
        with adbc_driver_manager.AdbcConnection(db) as conn:
            yield conn


def test_load_driver():
    # Fails, but in environments where we don't spin up testing
    # servers, this checks that we can at least *load* the driver
    with pytest.raises(adbc_driver_manager.ProgrammingError, match="account is empty"):
        with adbc_driver_snowflake.connect(""):
            pass


def test_query_trivial(snowflake):
    with adbc_driver_manager.AdbcStatement(snowflake) as stmt:
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all()


def test_options(snowflake):
    with adbc_driver_manager.AdbcStatement(snowflake) as stmt:
        stmt.set_options(
            **{
                adbc_driver_snowflake.StatementOptions.RESULT_QUEUE_SIZE.value: "1",
            }
        )
        stmt.set_sql_query("SELECT 1")
        stream, _ = stmt.execute_query()
        reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
        assert reader.read_all()


def test_version():
    assert adbc_driver_snowflake.__version__  # type:ignore
