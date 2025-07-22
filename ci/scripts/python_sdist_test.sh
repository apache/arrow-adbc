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

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <arch> <adbc-src-dir> <adbc-build-dir>"
  exit 1
fi

arch=${1}
source_dir=${2}
if [ "$#" -ge 3 ]; then
    build_dir=${3}
else
    build_dir="${source_dir}/build"
fi
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

source "${script_dir}/python_util.sh"

echo "=== Set up platform variables ==="
setup_build_vars "${arch}"

echo "=== Building C/C++ driver components ==="
build_drivers "${source_dir}" "${build_dir}"

echo "=== Installing sdists ==="
for component in ${COMPONENTS}; do
    pip install --no-deps --force-reinstall ${source_dir}/python/${component}/dist/*.tar.gz
done
# N.B. for now, we still support manylinux2014 while PyArrow dropped support.
# Explicitly install the last version of PyArrow supporting manylinux2014.
# - https://github.com/apache/arrow-adbc/issues/3182
# - https://github.com/apache/arrow/issues/46959
pip install importlib-resources pytest pyarrow==19.0.1 pandas polars protobuf

echo "=== (${PYTHON_VERSION}) Testing sdists ==="
test_packages
