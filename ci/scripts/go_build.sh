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

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local install_dir="${3}"

    if [[ -z "${install_dir}" ]]; then
        install_dir="${build_dir}/local"
    fi

    if [[ "${CGO_ENABLED}" = 1 ]]; then
        export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${install_dir}/lib"
        export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${install_dir}/lib"
    fi

    pushd "${source_dir}/go/adbc"

    go build -v ./...

    if [[ "${CGO_ENABLED}" = 1 ]]; then
        pushd ./pkg

        make all

        popd

        mkdir -p "${install_dir}/lib"
        if [[ $(go env GOOS) == "linux" ]]; then
            for lib in ./pkg/*.so; do
                cp "${lib}" "${install_dir}/lib"
            done
        else
            for lib in ./pkg/*.dylib; do
                cp "${lib}" "${install_dir}/lib"
            done
        fi
    fi

    popd
}

main "$@"
