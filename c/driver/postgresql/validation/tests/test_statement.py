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

import adbc_drivers_validation.tests.statement as test_statement
import pytest

from . import postgresql


class TestStatement(test_statement.TestStatement):
    def test_rows_affected(self, driver, conn) -> None:
        if isinstance(driver, postgresql.CrateDBQuirks):
            pytest.xfail(
                "CrateDB does not make writes immediately visible, and the upstream "
                "test provides no REFRESH TABLE hook"
            )
        super().test_rows_affected(driver, conn)


def pytest_generate_tests(metafunc) -> None:
    vendor = metafunc.config.getoption("vendor")
    return test_statement.generate_tests([postgresql.get_quirks(vendor)], metafunc)
