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
: ${BUILD_DRIVER_BIGQUERY:=${BUILD_ALL}}
: ${BUILD_DRIVER_FLIGHTSQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_MANAGER:=${BUILD_ALL}}
: ${BUILD_DRIVER_POSTGRESQL:=${BUILD_ALL}}
: ${BUILD_DRIVER_SQLITE:=${BUILD_ALL}}
: ${BUILD_DRIVER_SNOWFLAKE:=${BUILD_ALL}}

if [[ $(uname) = "Darwin" ]]; then
    ADBC_LIBRARY_SUFFIX="dylib"
else
    ADBC_LIBRARY_SUFFIX="so"
fi

install_pkg() {
    local -r source_dir="${1}"
    local -r install_dir="${2}"
    local -r pkg="${3}"
    R CMD INSTALL "${source_dir}/r/${pkg}" --preclean --library="${install_dir}"
}

main() {
    local -r source_dir="${1}"
    local -r install_dir="${2}"

    R_LIBS_USER="${install_dir}" R -e 'install.packages("nanoarrow", repos = "https://cloud.r-project.org/")' --vanilla

    if [[ "${BUILD_DRIVER_MANAGER}" -gt 0 ]]; then
        install_pkg "${source_dir}" "${install_dir}" adbcdrivermanager
    fi

    if [[ "${BUILD_DRIVER_FLIGHTSQL}" -gt 0 ]]; then
        install_pkg "${source_dir}" "${install_dir}" adbcflightsql
    fi

    if [[ "${BUILD_DRIVER_POSTGRESQL}" -gt 0 ]]; then
        install_pkg "${source_dir}" "${install_dir}" adbcpostgresql
    fi

    if [[ "${BUILD_DRIVER_SQLITE}" -gt 0 ]]; then
        install_pkg "${source_dir}" "${install_dir}" adbcsqlite
    fi

    if [[ "${BUILD_DRIVER_SNOWFLAKE}" -gt 0 ]]; then
        install_pkg "${source_dir}" "${install_dir}" adbcsnowflake
    fi

    if [[ "${BUILD_DRIVER_BIGQUERY}" -gt 0 ]]; then
        install_pkg "${source_dir}" "${install_dir}" adbcbigquery
    fi
}

main "$@"
