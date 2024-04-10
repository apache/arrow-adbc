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

: ${ADBC_BUILD_SHARED:=ON}
: ${ADBC_BUILD_STATIC:=OFF}
: ${ADBC_BUILD_TESTS:=ON}
: ${ADBC_USE_ASAN:=ON}
: ${ADBC_USE_UBSAN:=ON}

: ${ADBC_CMAKE_ARGS:=""}
: ${CMAKE_BUILD_TYPE:=Debug}

build_subproject() {
    local -r source_dir="${1}"
    local -r build_dir="${2}/tidy"

    mkdir -p "${build_dir}"
    pushd "${build_dir}"

    set -x

    # XXX(https://github.com/llvm/llvm-project/issues/46804): expand
    # CC and CXX symlinks
    env CC=$(readlink -f ${CC:-$(which cc)}) \
        CXX=$(readlink -f ${CXX:-$(which c++)}) \
        cmake "${source_dir}/c" \
        ${ADBC_CMAKE_ARGS} \
        -DADBC_BUILD_SHARED="${ADBC_BUILD_SHARED}" \
        -DADBC_BUILD_STATIC="${ADBC_BUILD_STATIC}" \
        -DADBC_BUILD_TESTS="${ADBC_BUILD_TESTS}" \
        -DADBC_DRIVER_MANAGER="${BUILD_DRIVER_MANAGER}" \
        -DADBC_DRIVER_POSTGRESQL="${BUILD_DRIVER_POSTGRESQL}" \
        -DADBC_DRIVER_SQLITE="${BUILD_DRIVER_SQLITE}" \
        -DADBC_DRIVER_FLIGHTSQL="${BUILD_DRIVER_FLIGHTSQL}" \
        -DADBC_DRIVER_SNOWFLAKE="${BUILD_DRIVER_SNOWFLAKE}" \
        -DADBC_USE_ASAN="${ADBC_USE_ASAN}" \
        -DADBC_USE_UBSAN="${ADBC_USE_UBSAN}" \
        -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX="${CMAKE_INSTALL_PREFIX}"

    run-clang-tidy \
        -extra-arg=-Wno-unknown-warning-option \
        -j $(nproc) \
        -p "${build_dir}" \
        -fix \
        -quiet \
        $(jq -r ".[] | .file" "${build_dir}/compile_commands.json")

    set +x
    popd
}

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"

    build_subproject "${source_dir}" "${build_dir}"
}

main "$@"
