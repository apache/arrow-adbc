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
  echo "Usage: $0 <adbc-src-dir>"
  exit 1
fi

source_dir=${1}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${script_dir}/python_util.sh"

echo "=== (${PYTHON_VERSION}) Installing wheels ==="

PYTHON_TAG=cp$(python -c "import sysconfig; print(sysconfig.get_python_version().replace('.', ''))")
PYTHON_FLAGS=$(python -c "import sysconfig; print(sysconfig.get_config_var('abiflags'))")

for component in ${COMPONENTS}; do
    if [[ "${component}" = "adbc_driver_manager" ]]; then
        # Don't pick up cp313-cp313t when we wanted cp313-cp313 or vice versa (for example)
        WHEEL_TAG="${PYTHON_TAG}-${PYTHON_TAG}${PYTHON_FLAGS}"
    else
        WHEEL_TAG=py3-none
    fi

    if [[ -d ${source_dir}/python/${component}/repaired_wheels/ ]]; then
        python -m pip install --no-deps --force-reinstall \
            ${source_dir}/python/${component}/repaired_wheels/*-${WHEEL_TAG}-*.whl
    elif [[ -d ${source_dir}/python/${component}/dist/ ]]; then
        python -m pip install --no-deps --force-reinstall \
            ${source_dir}/python/${component}/dist/*-${WHEEL_TAG}-*.whl
    else
        echo "NOTE: assuming wheels are already installed"
    fi
done

python -m pip install importlib-resources pytest pyarrow pandas protobuf
if [[ -z "${PYTHON_FLAGS}" ]]; then
    # polars does not support freethreading and will try to build from source
    python -m pip install polars
fi

echo "=== (${PYTHON_VERSION}) Testing wheels ==="
test_packages

if [[ -z "${PYTHON_FLAGS}" ]]; then
    echo "=== (${PYTHON_VERSION}) Testing wheels (no PyArrow) ==="
    python -m pip uninstall -y pyarrow
    export PYTEST_ADDOPTS="${PYTEST_ADDOPTS} -k pyarrowless"
    test_packages_pyarrowless
else
    echo "Freethreading build, skipping pyarrowless tests"
    echo "(polars does not yet support freethreading)"
fi
