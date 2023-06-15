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

from typing import Generator

import pytest

from adbc_driver_postgresql import dbapi


@pytest.fixture
def postgres(postgres_uri: str) -> Generator[dbapi.Connection, None, None]:
    with dbapi.connect(postgres_uri) as conn:
        yield conn


def test_query_trivial(postgres: dbapi.Connection):
    with postgres.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_ddl(postgres: dbapi.Connection):
    with postgres.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS test_ddl")
        assert cur.fetchone() is None

        cur.execute("CREATE TABLE test_ddl (ints INT)")
        assert cur.fetchone() is None

        cur.execute("INSERT INTO test_ddl VALUES (1) RETURNING ints")
        assert cur.fetchone() == (1,)

        cur.execute("SELECT * FROM test_ddl")
        assert cur.fetchone() == (1,)
