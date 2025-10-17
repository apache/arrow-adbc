#!/usr/bin/env bash
#
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

set -euo pipefail

main() {
    local -r source_root="${1}"
    local -r scratch="${2}"
    local -r sqlite_driver="${3}"

    mkdir -p "${scratch}"
    python -m venv "${scratch}/.venv"
    source "${scratch}/.venv/bin/activate"

    "${scratch}"/.venv/bin/python -m pip install "${source_root}"/python/adbc_driver_manager
    "${scratch}"/.venv/bin/python -m pip install pyarrow

    mkdir -p "${scratch}/.venv/etc/adbc/drivers/"
    cat >"${scratch}/.venv/etc/adbc/drivers/sqlite.toml" <<EOF
name = "SQLite"
[Driver]
shared = "${sqlite_driver}"
EOF

    cat >"${scratch}/test.py" <<EOF
import adbc_driver_manager.dbapi

with adbc_driver_manager.dbapi.connect(driver="sqlite") as con:
    with con.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
EOF

    "${scratch}"/.venv/bin/python "${scratch}/test.py"
    echo "PASSED: find manifest"

    cat >"${scratch}/test2.py" <<EOF
import adbc_driver_manager.dbapi

try:
    with adbc_driver_manager.dbapi.connect(driver="notfound") as con:
        pass
except adbc_driver_manager.dbapi.Error as e:
    print(e)
    assert ".venv/etc/adbc/drivers" in str(e)
else:
    assert False, "Expected exception"
EOF

    "${scratch}"/.venv/bin/python "${scratch}/test2.py"
    echo "PASSED: failed manifest contains the proper path in the exception"
}

main "$@"
