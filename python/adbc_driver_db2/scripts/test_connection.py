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
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Quick DB2 connectivity check via the ADBC DB2 driver.

Usage:
  export ADBC_DB2_TEST_URI='DATABASE=testdb;UID=db2inst1;PWD=password;HOSTNAME=localhost;PORT=50000;PROTOCOL=TCPIP'
  # Docker compose `db2-test` uses db2inst1; use db2inst2 if that matches your server.
  export IBM_DB_HOME=$(python -c "import importlib, pathlib; p = pathlib.Path(importlib.import_module('ibm_db').__file__).resolve().parent; print(p.parent / 'clidriver' if p.name == 'ibm_db' else p / 'clidriver')")
  export DYLD_LIBRARY_PATH="/path/to/arrow-adbc/build/driver/db2:${IBM_DB_HOME}/lib:${DYLD_LIBRARY_PATH}"   # macOS
  # Linux: set LD_LIBRARY_PATH the same way (include the CMake build output dir for libadbc_driver_db2.*).
  python scripts/test_connection.py

Optional: load .env from repo root:
  python -m dotenv -f ../../.env run -- python scripts/test_connection.py
"""

from __future__ import annotations

import os
import sys


def main() -> int:
    uri = os.environ.get("ADBC_DB2_TEST_URI")
    if not uri:
        print(
            "Set ADBC_DB2_TEST_URI to a DB2 CLI connection string, e.g.\n"
            '  DATABASE=testdb;UID=db2inst1;PWD=password;HOSTNAME=localhost;'
            "PORT=50000;PROTOCOL=TCPIP\n"
            "  (Docker compose db2-test uses db2inst1; adjust UID if needed.)",
            file=sys.stderr,
        )
        return 1

    try:
        import pyarrow
        import adbc_driver_db2
        import adbc_driver_manager as mgr
    except ImportError as e:
        print(
            "Install the package first, e.g. from repo root after build:\n"
            "  pip install -e python/adbc_driver_db2",
            file=sys.stderr,
        )
        print(e, file=sys.stderr)
        return 1

    print("Connecting with ADBC DB2 driver...")
    db = adbc_driver_db2.connect(uri)
    conn = mgr.AdbcConnection(db)
    stmt = mgr.AdbcStatement(conn)
    stmt.set_sql_query("SELECT 1 AS one FROM SYSIBM.SYSDUMMY1")
    stream, _rows = stmt.execute_query()
    reader = pyarrow.RecordBatchReader._import_from_c(stream.address)
    table = reader.read_all()
    print("OK:", table)
    stmt.close()
    conn.close()
    db.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
