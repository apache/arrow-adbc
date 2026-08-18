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

import adbc_drivers_validation.tests.connection as test_connection
import pytest

from . import postgresql


class TestConnection(test_connection.TestConnection):
    def test_get_objects_column_filter_table(
        self, conn, driver, get_objects_table
    ) -> None:
        if isinstance(driver, postgresql.CockroachDBQuirks):
            pytest.xfail(
                "CockroachDB has an implicit rowid column and does not preserve "
                "column order in GetObjects"
            )
        super().test_get_objects_column_filter_table(conn, driver, get_objects_table)

    def test_get_objects_column_filter_table_name(
        self, conn, driver, get_objects_table
    ) -> None:
        if isinstance(driver, postgresql.CockroachDBQuirks):
            pytest.xfail(
                "CockroachDB has an implicit rowid column and does not preserve "
                "column order in GetObjects"
            )
        super().test_get_objects_column_filter_table_name(
            conn, driver, get_objects_table
        )

    def test_get_objects_column_xdbc(self, conn, driver, get_objects_table) -> None:
        if isinstance(driver, postgresql.CockroachDBQuirks):
            pytest.xfail(
                "CockroachDB has an implicit rowid column and does not preserve "
                "column order in GetObjects"
            )
        super().test_get_objects_column_xdbc(conn, driver, get_objects_table)


def pytest_generate_tests(metafunc) -> None:
    vendor = metafunc.config.getoption("vendor")
    return test_connection.generate_tests([postgresql.get_quirks(vendor)], metafunc)
