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

from adbc_driver_manager import dbapi


@pytest.fixture
def sqlite():
    """Dynamically load the SQLite driver."""
    with dbapi.connect(
        driver="adbc_driver_sqlite",
        entrypoint="AdbcSqliteDriverInit",
    ) as conn:
        yield conn


def test_type_objects():
    assert dbapi.NUMBER == pyarrow.int64()
    assert pyarrow.int64() == dbapi.NUMBER

    assert dbapi.STRING == pyarrow.string()
    assert pyarrow.string() == dbapi.STRING


def test_query(sqlite):
    with sqlite.cursor() as cur:
        cur.execute('SELECT 1, "foo", 2.0')
        assert cur.description == [
            ("1", dbapi.NUMBER, None, None, None, None, None),
            ('"foo"', dbapi.STRING, None, None, None, None, None),
            ("2.0", dbapi.NUMBER, None, None, None, None, None),
        ]
        assert cur.fetchone() == (1, "foo", 2.0)
        assert cur.fetchone() is None
