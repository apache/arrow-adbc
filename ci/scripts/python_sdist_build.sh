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

set -ex

source_dir=${1}
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

git config --global --add safe.directory ${source_dir}

source "${script_dir}/python_util.sh"

echo "=== (${PYTHON_VERSION}) Building ADBC sdists ==="

# https://github.com/pypa/pip/issues/7555
# Get the latest pip so we have in-tree-build by default
pip install --upgrade pip build

# For drivers, which bundle shared libraries, defer that to install time
export _ADBC_IS_SDIST=1

for component in ${COMPONENTS}; do
    pushd ${source_dir}/python/$component

    echo "=== Building $component sdist ==="
    # python -m build copies to a tempdir, so we can't reference other files in the repo
    # https://github.com/pypa/pip/issues/5519
    python -m build --sdist .

    popd
done
