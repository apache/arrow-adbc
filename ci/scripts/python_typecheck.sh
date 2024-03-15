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
: ${BUILD_DRIVER_FLIGHTSQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_MANAGER:=${BUILD_ALL}}
: ${BUILD_DRIVER_POSTGRESQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_SQLITE:=${BUILD_ALL}}
: ${BUILD_DRIVER_SNOWFLAKE:=${BUILD_ALL}}

: ${PYRIGHT_OPTIONS:=""}

main() {
    local -r source_dir="${1}"

    local status=0

    if [[ "${BUILD_DRIVER_FLIGHTSQL}" -gt 0 ]]; then
        if ! pyright ${PYRIGHT_OPTIONS} "${source_dir}/python/adbc_driver_flightsql"; then
            status=1
        fi
    fi

    if [[ "${BUILD_DRIVER_MANAGER}" -gt 0 ]]; then
        if ! pyright ${PYRIGHT_OPTIONS} "${source_dir}/python/adbc_driver_manager"; then
            status=1
        fi
    fi

    if [[ "${BUILD_DRIVER_POSTGRESQL}" -gt 0 ]]; then
        if ! pyright ${PYRIGHT_OPTIONS} "${source_dir}/python/adbc_driver_postgresql"; then
            status=1
        fi
    fi

    if [[ "${BUILD_DRIVER_SQLITE}" -gt 0 ]]; then
        if ! pyright ${PYRIGHT_OPTIONS} "${source_dir}/python/adbc_driver_sqlite"; then
            status=1
        fi
    fi

    if [[ "${BUILD_DRIVER_SNOWFLAKE}" -gt 0 ]]; then
        if ! pyright ${PYRIGHT_OPTIONS} "${source_dir}/python/adbc_driver_snowflake"; then
            status=1
        fi
    fi

    return $status
}

main "$@"
