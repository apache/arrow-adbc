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
: ${BUILD_DRIVER_FLIGHTSQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_SNOWFLAKE:=${BUILD_ALL}}
: ${BUILD_DRIVER_BIGQUERY:=${BUILD_ALL}}
: ${BUILD_INTEGRATION_DUCKDB:=${BUILD_ALL}}

test_project() {
    local -r build_dir="${1}"

    pushd "${build_dir}/"

    local labels=""
    if [[ "${BUILD_DRIVER_BIGQUERY}" -gt 0 ]]; then
       labels="${labels}|driver-bigquery"
    fi
    if [[ "${BUILD_DRIVER_FLIGHTSQL}" -gt 0 ]]; then
       labels="${labels}|driver-flightsql"
    fi
    if [[ "${BUILD_DRIVER_MANAGER}" -gt 0 ]]; then
       labels="${labels}|driver-common|driver-manager"
    fi
    if [[ "${BUILD_DRIVER_POSTGRESQL}" -gt 0 ]]; then
       labels="${labels}|driver-postgresql"
    fi
    if [[ "${BUILD_DRIVER_SNOWFLAKE}" -gt 0 ]]; then
       labels="${labels}|driver-snowflake"
    fi
    if [[ "${BUILD_DRIVER_SQLITE}" -gt 0 ]]; then
       labels="${labels}|driver-sqlite"
    fi
    if [[ "${BUILD_INTEGRATION_DUCKDB}" -gt 0 ]]; then
       labels="${labels}|integration-duckdb"
    fi
    # Remove leading pipe
    labels="${labels:1}"

    ctest --progress -VV \
        --output-on-failure \
        --no-tests=error \
        -L "${labels}"

    popd
}

main() {
    local -r build_dir="${1}"
    local install_dir="${2}"

    if [[ -z "${install_dir}" ]]; then
        install_dir="${build_dir}/local"
    fi

    export DYLD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${install_dir}/lib"
    export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${install_dir}/lib"

    test_project "${build_dir}"
}

main "$@"
