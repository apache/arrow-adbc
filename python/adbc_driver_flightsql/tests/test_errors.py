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

import contextlib
import re
import secrets
import threading
import time

import google.protobuf.any_pb2 as any_pb2
import google.protobuf.wrappers_pb2 as wrappers_pb2
import pytest


def assert_detail(e):
    # Check that the expected error details are present
    found = set()
    for _, detail in e.details:
        anyproto = any_pb2.Any()
        anyproto.ParseFromString(detail)
        string = wrappers_pb2.StringValue()
        anyproto.Unpack(string)
        found.add(string.value)
    assert found == {"detail1", "detail2"}


def test_query_cancel(test_dbapi):
    with test_dbapi.cursor() as cur:
        cur.execute("forever")
        cur.adbc_cancel()
        with pytest.raises(
            test_dbapi.OperationalError,
            match=re.escape("CANCELLED: [FlightSQL] context canceled"),
        ):
            cur.fetchone()


def test_query_cancel_async(test_dbapi):
    with test_dbapi.cursor() as cur:
        cur.execute("forever")

        def _cancel():
            time.sleep(2)
            cur.adbc_cancel()

        t = threading.Thread(target=_cancel, daemon=True)
        t.start()

        with pytest.raises(
            test_dbapi.OperationalError,
            match=re.escape("CANCELLED: [FlightSQL] context canceled"),
        ):
            cur.fetchone()


def test_query_error_fetch(test_dbapi):
    with test_dbapi.cursor() as cur:
        cur.execute("error_do_get")
        # Match more exactly to make sure there's not unexpected junk in the string
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape("INVALID_ARGUMENT: [FlightSQL] expected error (DoGet)"),
        ):
            cur.fetch_arrow_table()

        cur.execute("error_do_get_detail")
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape("INVALID_ARGUMENT: [FlightSQL] expected error (DoGet)"),
        ) as excval:
            cur.fetch_arrow_table()
        assert_detail(excval.value)


@pytest.mark.xfail(reason="apache/arrow-adbc#1576")
def test_query_error_vendor_code(test_dbapi):
    with test_dbapi.cursor() as cur:
        cur.execute("error_do_get")
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape("INVALID_ARGUMENT: [FlightSQL] expected error (DoGet)"),
        ) as excval:
            cur.fetch_arrow_table()

        # TODO(https://github.com/apache/arrow-adbc/issues/1576): vendor code
        # is gRPC status code; 3 is gRPC INVALID_ARGUMENT
        assert excval.value.vendor_code == 3


def test_query_error_stream(test_dbapi):
    with test_dbapi.cursor() as cur:
        cur.execute("error_do_get_stream")
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape(
                "INVALID_ARGUMENT: [FlightSQL] expected stream error (DoGet)"
            ),
        ):
            cur.fetchone()
            cur.fetchone()

        cur.execute("error_do_get_stream_detail")
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape(
                "INVALID_ARGUMENT: [FlightSQL] expected stream error (DoGet)"
            ),
        ) as excval:
            cur.fetchone()
            cur.fetchone()
        assert_detail(excval.value)


def test_query_error_bind(test_dbapi):
    with test_dbapi.cursor() as cur:
        cur.adbc_prepare("error_do_put")
        with pytest.raises(
            test_dbapi.OperationalError,
            match=re.escape("UNKNOWN: [FlightSQL] expected error (DoPut)"),
        ):
            cur.execute("error_do_put", parameters=(1, "a"))

        cur.adbc_prepare("error_do_put_detail")
        with pytest.raises(
            test_dbapi.OperationalError,
            match=re.escape("UNKNOWN: [FlightSQL] expected error (DoPut)"),
        ) as excval:
            cur.execute("error_do_put_detail", parameters=(1, "a"))
        assert_detail(excval.value)


def test_query_error_create_prepared_statement(test_dbapi):
    with test_dbapi.cursor() as cur:
        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape("INVALID_ARGUMENT: [FlightSQL] expected error (DoAction)"),
        ):
            cur.adbc_prepare("error_create_prepared_statement")

        with pytest.raises(
            test_dbapi.ProgrammingError,
            match=re.escape("INVALID_ARGUMENT: [FlightSQL] expected error (DoAction)"),
        ) as excval:
            cur.adbc_prepare("error_create_prepared_statement_detail")
        assert_detail(excval.value)


def test_query_error_getflightinfo(test_dbapi):
    with test_dbapi.cursor() as cur:
        with pytest.raises(
            Exception,
            match=re.escape(
                "INVALID_ARGUMENT: [FlightSQL] expected error (GetFlightInfo)"
            ),
        ):
            cur.execute("error_get_flight_info")

        with pytest.raises(
            Exception,
            match=re.escape(
                "INVALID_ARGUMENT: [FlightSQL] expected error (GetFlightInfo)"
            ),
        ) as excval:
            cur.execute("error_get_flight_info_detail")
        assert_detail(excval.value)

        cur.adbc_prepare("error_get_flight_info")
        with pytest.raises(
            Exception,
            match=re.escape(
                "INVALID_ARGUMENT: [FlightSQL] expected error (GetFlightInfo)"
            ),
        ):
            cur.adbc_execute_partitions("error_get_flight_info")


def test_stateless_prepared_statement(test_dbapi) -> None:
    with test_dbapi.cursor() as cur:
        cur.adbc_prepare("stateless_prepared_statement")
        cur.execute("stateless_prepared_statement", parameters=[(1,)])


def test_header_propagation(test_dbapi) -> None:
    header = "x-trace"
    option = f"adbc.flight.sql.rpc.call_header.{header}"

    @contextlib.contextmanager
    def _trace(value):
        test_dbapi.adbc_connection.set_options(**{option: value})
        yield
        test_dbapi.adbc_connection.set_options(**{option: ""})

    getobjects = secrets.token_hex(16)
    with _trace(getobjects):
        with test_dbapi.adbc_get_objects():
            pass

    stmt = secrets.token_hex(16)
    with _trace(stmt):
        with test_dbapi.cursor() as cur:
            cur.execute("foo")
            cur.fetchall()

    prepared = secrets.token_hex(16)
    with _trace(prepared):
        with test_dbapi.cursor() as cur:
            cur.adbc_prepare("stateless_prepared_statement")
            cur.execute("stateless_prepared_statement", parameters=[(1,)])

    txn = secrets.token_hex(16)
    with _trace(txn):
        test_dbapi.adbc_connection.set_autocommit(False)
        test_dbapi.adbc_connection.set_autocommit(True)

    sess = secrets.token_hex(16)
    with _trace(sess):
        test_dbapi.adbc_connection.get_option("adbc.flight.sql.session.options")
        test_dbapi.adbc_connection.set_options(
            **{
                "adbc.flight.sql.session.option.foo": 2,
            }
        )

    with test_dbapi.cursor() as cur:
        cur.execute("recorded_headers")
        headers = [x for x in cur.fetchall() if x[1] == header]

    for method in [
        "GetFlightInfoCatalogs",
        "DoGetCatalogs",
        "GetFlightInfoDBSchemas",
        "DoGetDBSchemas",
        "GetFlightInfoTables",
        "DoGetTables",
    ]:
        assert (method, header, getobjects) in headers

    for method in [
        "CreatePreparedStatement",
        "ClosePreparedStatement",
        "GetFlightInfoPreparedStatement",
        "DoGetPreparedStatement",
    ]:
        assert (method, header, stmt) in headers

    for method in [
        "CreatePreparedStatement",
        "ClosePreparedStatement",
        "GetFlightInfoPreparedStatement",
        "DoGetPreparedStatement",
        "DoPutPreparedStatementQuery",
    ]:
        assert (method, header, prepared) in headers

    for method in [
        "BeginTransaction",
        "EndTransaction",
    ]:
        assert (method, header, txn) in headers

    for method in [
        "GetSessionOptions",
        "SetSessionOptions",
    ]:
        assert (method, header, sess) in headers
