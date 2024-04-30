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

import re
import threading

import google.protobuf.any_pb2 as any_pb2
import google.protobuf.wrappers_pb2 as wrappers_pb2
import pyarrow
import pyarrow.flight
import pytest

import adbc_driver_manager
from adbc_driver_flightsql import StatementOptions as FlightSqlStatementOptions
from adbc_driver_manager import StatementOptions

SCHEMA = pyarrow.schema([("ints", "int32")])


def test_incremental_error(test_dbapi) -> None:
    with test_dbapi.cursor() as cur:
        cur.adbc_statement.set_options(
            **{
                StatementOptions.INCREMENTAL.value: "true",
            }
        )
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape("[FlightSQL] expected error (PollFlightInfo)"),
        ) as exc_info:
            cur.adbc_execute_partitions("error_poll")

        found = set()
        for _, detail in exc_info.value.details:
            anyproto = any_pb2.Any()
            anyproto.ParseFromString(detail)
            string = wrappers_pb2.StringValue()
            anyproto.Unpack(string)
            found.add(string.value)
        assert found == {"detail1", "detail2"}

        # After an error, we can execute a different query.
        partitions, schema = cur.adbc_execute_partitions("finish_immediately")
        assert len(partitions) == 1
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(1.0)


def test_incremental_error_poll(test_dbapi) -> None:
    with test_dbapi.cursor() as cur:
        cur.adbc_statement.set_options(
            **{
                StatementOptions.INCREMENTAL.value: "true",
            }
        )
        partitions, schema = cur.adbc_execute_partitions("error_poll_later")
        assert len(partitions) == 1
        assert schema is None
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(0.2)

        # An error can be retried.
        with pytest.raises(
            test_dbapi.OperationalError,
            match=re.escape("[FlightSQL] expected error (PollFlightInfo)"),
        ) as excinfo:
            partitions, schema = cur.adbc_execute_partitions("error_poll_later")
        assert excinfo.value.status_code == adbc_driver_manager.AdbcStatusCode.IO

        partitions, schema = cur.adbc_execute_partitions("error_poll_later")
        assert len(partitions) == 2
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(0.6)

        partitions, schema = cur.adbc_execute_partitions("error_poll_later")
        assert len(partitions) == 1
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(0.8)

        partitions, schema = cur.adbc_execute_partitions("error_poll_later")
        assert len(partitions) == 1
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(1.0)

        partitions, _ = cur.adbc_execute_partitions("error_poll_later")
        assert partitions == []


def test_incremental_cancel(test_dbapi) -> None:
    with test_dbapi.cursor() as cur:
        assert (
            cur.adbc_statement.get_option_bytes(
                FlightSqlStatementOptions.LAST_FLIGHT_INFO.value
            )
            == b""
        )

        cur.adbc_statement.set_options(
            **{
                StatementOptions.INCREMENTAL.value: "true",
            }
        )
        partitions, schema = cur.adbc_execute_partitions("forever")
        assert len(partitions) == 1

        passed = False

        def _bg():
            nonlocal passed
            while True:
                progress = cur.adbc_statement.get_option_float(
                    StatementOptions.PROGRESS.value
                )
                # XXX: upstream PyArrow never bothered exposing app_metadata
                raw_info = cur.adbc_statement.get_option_bytes(
                    FlightSqlStatementOptions.LAST_FLIGHT_INFO.value
                )

                # check that it's a valid info
                pyarrow.flight.FlightInfo.deserialize(raw_info)
                passed = b"app metadata" in raw_info

                if progress > 0.07:
                    break
            cur.adbc_cancel()

        t = threading.Thread(target=_bg, daemon=True)
        t.start()

        with pytest.raises(test_dbapi.OperationalError, match="(?i)cancelled"):
            cur.adbc_execute_partitions("forever")

        t.join()
        assert passed


def test_incremental_immediately(test_dbapi) -> None:
    with test_dbapi.cursor() as cur:
        cur.adbc_statement.set_options(
            **{
                StatementOptions.INCREMENTAL.value: "true",
            }
        )
        partitions, schema = cur.adbc_execute_partitions("finish_immediately")
        assert len(partitions) == 1
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(1.0)

        partitions, schema = cur.adbc_execute_partitions("finish_immediately")
        assert partitions == []

        # reuse for a new query
        partitions, schema = cur.adbc_execute_partitions("finish_immediately")
        assert len(partitions) == 1
        partitions, schema = cur.adbc_execute_partitions("finish_immediately")
        assert partitions == []


def test_incremental_query(test_dbapi) -> None:
    with test_dbapi.cursor() as cur:
        cur.adbc_statement.set_options(
            **{
                StatementOptions.INCREMENTAL.value: "true",
            }
        )
        partitions, schema = cur.adbc_execute_partitions("SELECT 1")
        assert len(partitions) == 1
        assert schema is None
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(0.2)

        message = (
            "[Flight SQL] Cannot disable incremental execution "
            "while a query is in progress"
        )
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape(message),
        ) as excinfo:
            cur.adbc_statement.set_options(
                **{
                    StatementOptions.INCREMENTAL.value: "false",
                }
            )
        assert (
            excinfo.value.status_code
            == adbc_driver_manager.AdbcStatusCode.INVALID_STATE
        )

        partitions, schema = cur.adbc_execute_partitions("SELECT 1")
        assert len(partitions) == 1
        assert schema is None
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(0.4)

        partitions, schema = cur.adbc_execute_partitions("SELECT 1")
        assert len(partitions) == 1
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(0.6)

        partitions, schema = cur.adbc_execute_partitions("SELECT 1")
        assert len(partitions) == 1
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(0.8)

        partitions, schema = cur.adbc_execute_partitions("SELECT 1")
        assert len(partitions) == 1
        assert schema == SCHEMA
        assert cur.adbc_statement.get_option_float(
            StatementOptions.PROGRESS.value
        ) == pytest.approx(1.0)

        partitions, schema = cur.adbc_execute_partitions("SELECT 1")
        assert len(partitions) == 0
        assert schema == SCHEMA
        assert (
            cur.adbc_statement.get_option_float(StatementOptions.PROGRESS.value) == 0.0
        )

        cur.adbc_statement.set_options(
            **{
                StatementOptions.INCREMENTAL.value: "false",
            }
        )
