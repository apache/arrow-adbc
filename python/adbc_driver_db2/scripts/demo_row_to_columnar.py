#!/usr/bin/env python3
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
# software distributed under this License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Create a classic relational (row-oriented) table in DB2, then read it as Arrow columnar data.

DB2 stores rows on disk; the ADBC driver fetches via CLI and builds Apache Arrow column vectors.

Usage (same env as ``test_connection.py``):

  export ADBC_DB2_TEST_URI='DATABASE=testdb;UID=db2inst2;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP'
  export IBM_DB_HOME=$(python -c "import importlib, pathlib; p = pathlib.Path(importlib.import_module('ibm_db').__file__).resolve().parent; print(p.parent / 'clidriver' if p.name == 'ibm_db' else p / 'clidriver')")
  export DYLD_LIBRARY_PATH="/path/to/arrow-adbc/build/driver/db2:${IBM_DB_HOME}/lib:${DYLD_LIBRARY_PATH}"

  python scripts/demo_row_to_columnar.py

Optional table name override:

  export ADBC_DB2_DEMO_TABLE=MY_DEMO_TABLE
"""

from __future__ import annotations

import os
import sys


def main() -> int:
    uri = os.environ.get("ADBC_DB2_TEST_URI")
    if not uri:
        print(
            "Set ADBC_DB2_TEST_URI to a DB2 CLI connection string.",
            file=sys.stderr,
        )
        return 1

    table_name = os.environ.get("ADBC_DB2_DEMO_TABLE", "ADBC_DEMO_ROW_COL")

    try:
        from adbc_driver_db2 import dbapi
    except ImportError as e:
        print(
            "Install the package first, e.g.\n"
            "  pip install -e python/adbc_driver_db2",
            file=sys.stderr,
        )
        print(e, file=sys.stderr)
        return 1

    print(f"Using relational table (row store): {table_name}")
    print("(DB2 stores rows; Arrow is columnar — the driver converts on read.)\n")

    with dbapi.connect(uri) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute(f"DROP TABLE {table_name}")
            except dbapi.ProgrammingError:
                pass

            cur.execute(
                f"""
                CREATE TABLE {table_name} (
                    ID INTEGER NOT NULL,
                    NAME VARCHAR(40),
                    UNITS DOUBLE,
                    CREATED DATE
                )
                """
            )

            # Row-oriented inserts (classic relational rows).
            cur.execute(
                f"""
                INSERT INTO {table_name} (ID, NAME, UNITS, CREATED) VALUES
                  (1, 'alpha', 10.5, DATE '2025-01-15'),
                  (2, 'beta',  20.0, DATE '2025-02-01'),
                  (3, 'gamma', 3.25, DATE '2025-03-10')
                """
            )

        conn.commit()

        with conn.cursor() as cur:
            cur.execute(
                f"SELECT ID, NAME, UNITS, CREATED FROM {table_name} ORDER BY ID"
            )
            arrow_table = cur.fetch_arrow_table()

    print("Apache Arrow table (columnar):")
    print(arrow_table)
    print("\nColumn arrays (columnar layout):")
    for name in arrow_table.column_names:
        col = arrow_table.column(name)
        print(f"  {name}: {col.to_pylist()}")

    try:
        df = arrow_table.to_pandas()
        print("\nPandas DataFrame (still column-oriented by column):")
        print(df)
    except Exception as exc:  # noqa: BLE001 — optional dependency path
        print(f"\n(to_pandas skipped: {exc})")

    print(
        f"\nCleanup: DROP TABLE {table_name}; (not run automatically — "
        "re-run script drops it at start.)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
