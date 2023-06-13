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

from pathlib import Path

import pytest

from adbc_driver_sqlite import dbapi


@pytest.fixture
def sqlite():
    with dbapi.connect() as conn:
        yield conn


def test_query_trivial(sqlite) -> None:
    with sqlite.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchone() == (1,)


def test_autocommit(tmp_path: Path) -> None:
    # apache/arrow-adbc#599
    db = tmp_path / "tmp.sqlite"
    with dbapi.connect(f"file:{db}") as conn:
        assert not conn._autocommit
        with conn.cursor() as cur:
            with pytest.raises(
                dbapi.OperationalError,
                match="cannot change into wal mode from within a transaction",
            ):
                cur.execute("PRAGMA journal_mode = WAL")

    # This now works if we enable autocommit
    with dbapi.connect(f"file:{db}", autocommit=True) as conn:
        assert conn._autocommit
        with conn.cursor() as cur:
            cur.execute("PRAGMA journal_mode = WAL")

    # Or we can use executescript
    with dbapi.connect(f"file:{db}") as conn:
        assert not conn._autocommit
        with conn.cursor() as cur:
            cur.executescript("PRAGMA journal_mode = WAL")
