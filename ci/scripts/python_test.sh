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

: ${ADBC_USE_ASAN:=OFF}
: ${ADBC_USE_UBSAN:=OFF}
: ${BUILD_ALL:=1}
: ${BUILD_DRIVER_BIGQUERY:=${BUILD_ALL}}
: ${BUILD_DRIVER_FLIGHTSQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_MANAGER:=${BUILD_ALL}}
: ${BUILD_DRIVER_POSTGRESQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_SQLITE:=${BUILD_ALL}}
: ${BUILD_DRIVER_SNOWFLAKE:=${BUILD_ALL}}
: ${CC:=gcc}
: ${PYTHONDEVMODE:=1}

test_subproject() {
    local -r source_dir=${1}
    local -r install_dir=${2}
    local -r subproject=${3}

    local options=()
    local preload=()

    if [[ "${subproject}" == "adbc_driver_manager" ]]; then
        options+=(LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${install_dir}/lib")
        options+=(DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${install_dir}/lib")
    fi

    if [[ "${ADBC_USE_ASAN}" != "OFF" ]]; then
        preload+=($(${CC} --print-file-name=libasan.so))
        # CPython itself seems to leak
        options+=(ASAN_OPTIONS=detect_leaks=0)
    fi

    if [[ "${ADBC_USE_UBSAN}" != "OFF" ]]; then
        preload+=($(${CC} --print-file-name=libubsan.so))
    fi

    if [[ "${#preload[@]}" -gt 0 ]]; then
        local v=$(printf ":%s" "${preload[@]}")
        options+=(LD_PRELOAD=${v:1})
    fi

    echo "=== Testing ${subproject} ==="
    echo env ${options[@]} python -m pytest -vvs --full-trace "${source_dir}/python/${subproject}/tests"
    env ${options[@]} python -m pytest -vvs --full-trace "${source_dir}/python/${subproject}/tests"
    echo
}

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local install_dir="${3}"

    if [[ -z "${install_dir}" ]]; then
        install_dir="${build_dir}/local"
    fi

    if [[ "${BUILD_DRIVER_BIGQUERY}" -gt 0 ]]; then
        test_subproject "${source_dir}" "${install_dir}" adbc_driver_bigquery
    fi

    if [[ "${BUILD_DRIVER_FLIGHTSQL}" -gt 0 ]]; then
        test_subproject "${source_dir}" "${install_dir}" adbc_driver_flightsql
    fi

    if [[ "${BUILD_DRIVER_MANAGER}" -gt 0 ]]; then
        test_subproject "${source_dir}" "${install_dir}" adbc_driver_manager
    fi

    if [[ "${BUILD_DRIVER_POSTGRESQL}" -gt 0 ]]; then
        test_subproject "${source_dir}" "${install_dir}" adbc_driver_postgresql
    fi

    if [[ "${BUILD_DRIVER_SQLITE}" -gt 0 ]]; then
        test_subproject "${source_dir}" "${install_dir}" adbc_driver_sqlite
    fi

    if [[ "${BUILD_DRIVER_SNOWFLAKE}" -gt 0 ]]; then
        test_subproject "${source_dir}" "${install_dir}" adbc_driver_snowflake
    fi
}

main "$@"
