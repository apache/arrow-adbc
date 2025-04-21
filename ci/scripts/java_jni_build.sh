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

set -ex

: ${ADBC_CMAKE_ARGS:=""}
: ${CMAKE_BUILD_TYPE:=Debug}

main() {
    local -r source_dir=${1}
    local -r build_dir=${2}
    local -r install_dir=${3}

    mkdir -p "${build_dir}"
    pushd "${build_dir}"

    set -x
    cmake "${source_dir}/java" \
        ${ADBC_CMAKE_ARGS} \
        -DCMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE}" \
        -DCMAKE_INSTALL_PREFIX="${source_dir}/java/driver/jni/src/main/resources/" \
        -DCMAKE_PREFIX_PATH="${install_dir}/lib/cmake/"
    set +x

    cmake --build . --target install -j

    popd
}

main "$@"
