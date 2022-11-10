#!/usr/bin/env bash
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

: ${BUILD_ALL:=1}
: ${BUILD_DRIVER_MANAGER:=${BUILD_ALL}}
: ${BUILD_DRIVER_POSTGRESQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_SQLITE:=${BUILD_ALL}}

test_subproject() {
    local -r build_dir="${1}"
    local -r subproject="${2}"

    echo "=== Testing ${subproject} ==="

    pushd "${build_dir}/${subproject}"

    # macOS will not propagate DYLD_LIBRARY_PATH through a subprocess
    "./adbc-$(echo ${subproject} | sed "s|[/_]|-|g")-test"

    popd
}

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local install_dir="${3}"

    if [[ -z "${install_dir}" ]]; then
        install_dir="${build_dir}/local"
    fi

    export DYLD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${install_dir}/lib"
    export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${install_dir}/lib"

    if [[ "${BUILD_DRIVER_MANAGER}" -gt 0 ]]; then
        test_subproject "${build_dir}" driver_manager
    fi

    if [[ "${BUILD_DRIVER_POSTGRESQL}" -gt 0 ]]; then
        test_subproject "${build_dir}" driver/postgresql
    fi

    if [[ "${BUILD_DRIVER_SQLITE}" -gt 0 ]]; then
        test_subproject "${build_dir}" driver/sqlite
    fi
}

main "$@"
