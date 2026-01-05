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

# Run validation test suites.

set -euo pipefail

: ${BUILD_ALL:=1}
: ${BUILD_DRIVER_POSTGRESQL:=${BUILD_ALL}}

: ${ADBC_USE_ASAN:=OFF}
: ${ADBC_USE_UBSAN:=OFF}
: ${CC:=cc}

test_project() {
    local -r subdir="${1}"
    local -r run=$(echo "${2}" | tr '[:upper:]' '[:lower:]')

    case "${run}" in
        1|on)
            ;;
        *)
            echo "Skipping ${subdir}: ${run}"
            return 0
            ;;
    esac

    local options=()
    local preload=()

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

    pushd "${subdir}"
    echo env ${options[@]} pytest -vvs --junit-xml=validation-report.xml -rfEsxX tests/
    env ${options[@]} pytest -vvs --junit-xml=validation-report.xml -rfEsxX tests/
    popd
}

main() {
    local -r source_dir="${1}"

    test_project "${source_dir}/c/driver/postgresql/validation" "${BUILD_DRIVER_POSTGRESQL}"
}

main "$@"
