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
import typing

from adbc_drivers_validation import model
from adbc_drivers_validation.tests.connection import (
    TestConnection as BaseTestConnection,
)
from adbc_drivers_validation.tests.connection import (
    generate_tests,
)

import adbc_driver_manager.dbapi

from . import postgresql


class TestConnection(BaseTestConnection):
    """PostgreSQL-specific connection tests with overrides."""

    def test_get_info(
        self,
        driver: model.DriverQuirks,
        conn: adbc_driver_manager.dbapi.Connection,
        record_property: typing.Callable[[str, typing.Any], None],
    ) -> None:
        """Override to accept (unknown) as valid driver version."""
        info = conn.adbc_get_info()
        driver_version = info.get("driver_version")
        # PostgreSQL driver returns "(unknown)" instead of "unknown"
        assert (
            driver_version.startswith("v")
            or driver_version == "unknown"
            or driver_version == "unknown-dirty"
            or driver_version == "(unknown)"
        )
        record_property("driver_version", driver_version)
        assert info.get("driver_name") == driver.driver_name
        assert info.get("vendor_name") == driver.vendor_name
        vendor_version = info.get("vendor_version", "")
        if isinstance(driver.vendor_version, re.Pattern):
            assert driver.vendor_version.match(
                vendor_version
            ), f"{vendor_version!r} does not match {driver.vendor_version!r}"
        else:
            assert vendor_version == driver.vendor_version

        # PostgreSQL returns arrow version without 'v' prefix
        arrow_version = info.get("driver_arrow_version")
        assert arrow_version.startswith("v") or arrow_version[0].isdigit()

        record_property("vendor_version", vendor_version)
        record_property("short_version", driver.short_version)


def pytest_generate_tests(metafunc) -> None:
    return generate_tests(postgresql.QUIRKS, metafunc)
