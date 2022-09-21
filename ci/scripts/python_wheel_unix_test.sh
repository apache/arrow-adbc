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

set -e
set -x
set -o pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <arrow-src-dir>"
  exit 1
fi

source_dir=${1}

# Install the built wheels
pip install --force-reinstall ${source_dir}/python/adbc_driver_manager/repaired_wheels/*.whl
pip install --force-reinstall ${source_dir}/python/adbc_driver_postgres/repaired_wheels/*.whl
pip install pytest pyarrow pandas

# Test that the modules are importable
python -c "
import adbc_driver_manager
import adbc_driver_manager.dbapi
import adbc_driver_postgres
import adbc_driver_postgres.dbapi
"

# Can't yet run tests (need Postgres database, SQLite driver)
# for component in adbc_driver_manager adbc_driver_postgres; do
#     python -m pytest -vvx ${source_dir}/python/$component/tests
# done
