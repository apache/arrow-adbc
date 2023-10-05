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

import adbc_driver_flightsql.dbapi
import adbc_driver_manager


def test_query_error(dremio_dbapi):
    with dremio_dbapi.cursor() as cur:
        with pytest.raises(adbc_driver_flightsql.dbapi.ProgrammingError) as exc_info:
            cur.execute("SELECT")

        exc = exc_info.value
        assert exc.status_code == adbc_driver_manager.AdbcStatusCode.INVALID_ARGUMENT
        # Try to keep noise in exceptions minimal
        assert exc.args[0].startswith("INVALID_ARGUMENT: [FlightSQL] ")


def test_query_trivial(dremio_dbapi):
    with dremio_dbapi.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_query_partitioned(dremio_dbapi):
    with dremio_dbapi.cursor() as cur:
        partitions, schema = cur.adbc_execute_partitions("SELECT 1")
        assert len(partitions) == 1
        assert schema.equals(pyarrow.schema([("EXPR$0", "int32")]))

        cur.adbc_read_partition(partitions[0])
        assert cur.fetchone() == (1,)


def test_set_options(dremio_uri, dremio_user, dremio_pass):
    # Regression test for apache/arrow-adbc#713
    with adbc_driver_flightsql.dbapi.connect(
        dremio_uri,
        db_kwargs={
            adbc_driver_manager.DatabaseOptions.USERNAME.value: dremio_user,
            adbc_driver_manager.DatabaseOptions.PASSWORD.value: dremio_pass,
        },
        conn_kwargs={
            "adbc.flight.sql.rpc.call_header.x-foo": "1",
        },
    ):
        pass
