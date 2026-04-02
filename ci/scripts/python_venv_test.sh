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

    mkdir -p "${scratch}/.venv/etc/adbc/profiles/sqlite/"
    cat >"${scratch}/.venv/etc/adbc/profiles/sqlite/dev.toml" <<EOF
profile_version = 1
driver = "sqlite"
[Options]
uri = "file:///tmp/test.db"
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

    cat >"${scratch}/test3.py" <<EOF
import adbc_driver_manager.dbapi

with adbc_driver_manager.dbapi.connect(profile="sqlite/dev") as con:
    with con.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
EOF

    "${scratch}"/.venv/bin/python "${scratch}/test3.py"
    test -f /tmp/test.db
    echo "PASSED: find profile"

    # If we specify the additional path explicitly, then that overrides the
    # virtualenv path
    cat >"${scratch}/test4.py" <<EOF
import adbc_driver_manager.dbapi

db_kwargs = {
    "additional_profile_search_path_list": "/",
}
with adbc_driver_manager.dbapi.connect(profile="sqlite/dev", db_kwargs=db_kwargs) as con:
    with con.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
EOF

    if "${scratch}"/.venv/bin/python "${scratch}/test4.py"; then
        echo "FAILED: override profile path"
        exit 1
    fi
    echo "PASSED: override profile path"

    cat >"${scratch}/test5.py" <<EOF
import adbc_driver_manager.dbapi

db_kwargs = {
    "additional_manifest_search_path_list": "/",
}
with adbc_driver_manager.dbapi.connect(driver="sqlite", db_kwargs=db_kwargs) as con:
    with con.cursor() as cur:
        cur.execute("SELECT 1")
        assert cur.fetchall() == [(1,)]
EOF

    if "${scratch}"/.venv/bin/python "${scratch}/test5.py"; then
        echo "FAILED: override manifest path"
        exit 1
    fi
    echo "PASSED: override manifest path"
}

main "$@"
