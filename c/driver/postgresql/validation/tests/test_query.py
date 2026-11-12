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

import decimal

import adbc_drivers_validation.utils
import hypothesis
import hypothesis.strategies as st
import pyarrow
import pytest
from adbc_drivers_validation.tests.query import (
    TestQuery,  # noqa: F401
    generate_tests,
)

from . import postgresql


def pytest_generate_tests(metafunc) -> None:
    vendor = metafunc.config.getoption("vendor")
    quirks = [postgresql.get_quirks(vendor)]
    if not metafunc.definition.name.startswith("test_hypothesis_"):
        return generate_tests(quirks, metafunc)
    return adbc_drivers_validation.utils.generate_tests_by_marks(quirks, metafunc)


@st.composite
def st_decimal(draw, precision, scale) -> decimal.Decimal:
    bound = 10**precision - 1
    min_value = -bound
    max_value = bound
    unscaled = draw(st.integers(min_value=min_value, max_value=max_value))
    return decimal.Decimal(unscaled).scaleb(-scale, context=decimal.Context(40))


@pytest.mark.parametrize(
    "ps",
    [
        (1, 0),
        (10, 5),
        (38, 0),
        (38, 10),
        (38, 37),
    ],
    ids=lambda ps: f"precision_{ps[0]:02}_scale_{ps[1]:02}",
)
@hypothesis.given(data=st.data())
@hypothesis.settings(deadline=5000)
@pytest.mark.requires_features(["statement_bulk_ingest"])
def test_hypothesis_decimal128(driver, conn, ps: tuple[int, int], data) -> None:
    if driver.name == "cedardb" and ps[0] == 38:
        # Not confirmed, but it appears CedarDB overflows internally when
        # parsing large decimal values
        pytest.skip("apparent bug in CedarDB with precision = 38")

    dec = data.draw(st_decimal(*ps))
    arr = pyarrow.array([dec], type=pyarrow.decimal128(ps[0], ps[1]))
    arr.validate(full=True)
    assert arr[0].as_py() == dec
    tbl = pyarrow.Table.from_arrays([arr], names=["col"])

    temp_table = "temp_decimal128"
    with conn.cursor() as cur:
        if driver.name == "cedardb":
            # the driver doesn't add precision/scale when ingesting, but
            # CedarDB defaults to (38, 6) apparently which trips up tests
            cur.execute("DROP TABLE IF EXISTS temp_decimal128")
            cur.execute(f"CREATE TABLE temp_decimal128 (col DECIMAL({ps[0]}, {ps[1]}))")
            cur.adbc_ingest(temp_table, tbl, mode="append")
        else:
            cur.adbc_ingest(temp_table, tbl, mode="replace")
        cur.execute(f"SELECT col FROM {temp_table}")
        res = cur.fetchallarrow()

    assert res.num_rows == 1
    assert decimal.Decimal(res["col"][0].as_py()) == dec
