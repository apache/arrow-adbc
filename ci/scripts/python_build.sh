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

: ${ADBC_USE_ASAN:=ON}
: ${ADBC_USE_UBSAN:=ON}

: ${ADBC_CMAKE_ARGS:=""}
: ${CMAKE_BUILD_TYPE:=Debug}

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"

    set -x

    cmake -S "${source_dir}/c" \
          -B ${build_dir} \
          ${ADBC_CMAKE_ARGS} \
          -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
          -DADBC_USE_ASAN="${ADBC_USE_ASAN}" \
          -DADBC_USE_UBSAN="${ADBC_USE_UBSAN}" \
          -DADBC_DRIVER_BIGQUERY=${BUILD_DRIVER_BIGQUERY} \
          -DADBC_DRIVER_MANAGER=${BUILD_DRIVER_MANAGER} \
          -DADBC_DRIVER_FLIGHTSQL=${BUILD_DRIVER_FLIGHTSQL} \
          -DADBC_DRIVER_POSTGRESQL=${BUILD_DRIVER_POSTGRESQL} \
          -DADBC_DRIVER_SQLITE=${BUILD_DRIVER_SQLITE} \
          -DADBC_DRIVER_SNOWFLAKE=${BUILD_DRIVER_SNOWFLAKE} \
          -DADBC_BUILD_PYTHON=ON
    cmake --build ${build_dir} --target python

    set +x
}

main "$@"
