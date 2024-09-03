#!/bin/bash
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

set -euxo pipefail

echo "Using debug Python ${PYTHON}"

git config --global --add safe.directory /adbc

# https://github.com/mamba-org/mamba/issues/3289
cat /adbc/ci/conda_env_cpp.txt /adbc/ci/conda_env_python.txt |\
    grep -v -e '^$' |\
    grep -v -e '^#' |\
    sort |\
    tee /tmp/spec.txt

micromamba install -c conda-forge -y \
           -f /tmp/spec.txt \
           "conda-forge/label/python_debug::python=${PYTHON}[build=*_cpython]"
micromamba clean --all -y

export ADBC_USE_ASAN=ON
export ADBC_USE_UBSAN=ON

env ADBC_BUILD_TESTS=OFF /adbc/ci/scripts/cpp_build.sh /adbc /adbc/build/pydebug
/adbc/ci/scripts/python_build.sh /adbc /adbc/build/pydebug
/adbc/ci/scripts/python_test.sh /adbc /adbc/build/pydebug
