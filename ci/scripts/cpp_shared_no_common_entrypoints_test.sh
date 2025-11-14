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

# Test building a simple application using the static drivers, ensuring that
# symbols do not clash and all dependencies are present.
# https://github.com/apache/arrow-adbc/issues/2562

set -euo pipefail

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local -r install_dir="${3}"

    mkdir -p "${build_dir}"

    echo "Install ADBC"
    mkdir -p "${build_dir}/install"
    pushd "${build_dir}/install"
    env \
        ADBC_BUILD_SHARED=ON \
        ADBC_BUILD_STATIC=OFF \
        ADBC_BUILD_TESTS=OFF \
        ADBC_USE_ASAN=OFF \
        ADBC_USE_UBSAN=OFF \
        ADBC_CMAKE_ARGS="-DADBC_DEFINE_COMMON_ENTRYPOINTS=OFF" \
        "${source_dir}/ci/scripts/cpp_build.sh" \
        "${source_dir}" \
        "${build_dir}/install" \
        "${install_dir}"
    popd

    # No static libraries should exist
    if find "${install_dir}" | grep -E "libadbc.*\.a"; then
        echo "Found ADBC static libraries, which should not exist for this test"
        return 1
    fi

    # Make sure Adbc symbols aren't exported
    failed=0
    for lib in "${install_dir}/lib"/libadbc_driver_*.so; do
        echo "Checking symbols in $lib"
        if [[ $(basename "$lib") == *"adbc_driver_manager"* ]]; then
            continue
        fi

        nm \
            --defined-only \
            --demangle \
            --extern-only \
            --no-weak \
            "${lib}" | \
            grep ' T ' | \
            awk '{print $3}' | \
            { grep --extended-regexp '^Adbc' || true; } | \
            { grep --extended-regexp --invert-match '^AdbcDriver[a-zA-Z]+Init' || true; } \
            > /tmp/symbols.txt
        if [[ -s /tmp/symbols.txt ]]; then
            echo "Found unexpected exported Adbc* symbols in $lib"
            cat /tmp/symbols.txt
            failed=$(($failed + 1))
        fi
    done
    if [[ $failed -ne 0 ]]; then
        return $failed
    fi

    echo "Build test application"
    mkdir -p "${build_dir}/test"
    pushd "${build_dir}/test"
    cmake "${source_dir}/c/integration/shared_test" \
          -DCMAKE_PREFIX_PATH="${install_dir}" \
          -DCMAKE_VERBOSE_MAKEFILE=ON
    cmake --build "${build_dir}/test" --target shared_test

    env LD_LIBRARY_PATH="${install_dir}/lib" "${build_dir}/test/shared_test"
    popd
}

main "$@"
