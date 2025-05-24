#!/usr/bin/env bash
#
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

COMPONENTS="adbc_driver_bigquery adbc_driver_manager adbc_driver_flightsql adbc_driver_postgresql adbc_driver_sqlite adbc_driver_snowflake"

function find_drivers {
    local -r build_dir="${1}/${VCPKG_ARCH}"

    if [[ $(uname) == "Linux" ]]; then
        export ADBC_BIGQUERY_LIBRARY=${build_dir}/lib/libadbc_driver_bigquery.so
        export ADBC_FLIGHTSQL_LIBRARY=${build_dir}/lib/libadbc_driver_flightsql.so
        export ADBC_POSTGRESQL_LIBRARY=${build_dir}/lib/libadbc_driver_postgresql.so
        export ADBC_SQLITE_LIBRARY=${build_dir}/lib/libadbc_driver_sqlite.so
        export ADBC_SNOWFLAKE_LIBRARY=${build_dir}/lib/libadbc_driver_snowflake.so
    else # macOS
        export ADBC_BIGQUERY_LIBRARY=${build_dir}/lib/libadbc_driver_bigquery.dylib
        export ADBC_FLIGHTSQL_LIBRARY=${build_dir}/lib/libadbc_driver_flightsql.dylib
        export ADBC_POSTGRESQL_LIBRARY=${build_dir}/lib/libadbc_driver_postgresql.dylib
        export ADBC_SQLITE_LIBRARY=${build_dir}/lib/libadbc_driver_sqlite.dylib
        export ADBC_SNOWFLAKE_LIBRARY=${build_dir}/lib/libadbc_driver_snowflake.dylib
    fi
}

function build_drivers {
    local -r source_dir="$1"
    local -r build_dir="$2/${VCPKG_ARCH}"

    : ${CMAKE_BUILD_TYPE:=RelWithDebInfo}
    : ${CMAKE_UNITY_BUILD:=ON}
    : ${CMAKE_GENERATOR:=Ninja}
    : ${VCPKG_ROOT:=/opt/vcpkg}
    # Enable manifest mode
    : ${VCPKG_FEATURE_FLAGS:=manifests}
    : ${ADBC_DRIVER_SNOWFLAKE:=ON}
    # Add our custom triplets
    export VCPKG_OVERLAY_TRIPLETS="${source_dir}/ci/vcpkg/triplets/"

    find_drivers "${2}"

    if [[ $(uname) == "Linux" ]]; then
        export VCPKG_DEFAULT_TRIPLET="${VCPKG_ARCH}-linux-static-release"
        export CMAKE_ARGUMENTS=""
    else # macOS
        export VCPKG_DEFAULT_TRIPLET="${VCPKG_ARCH}-osx-static-release"
        if [[ "${VCPKG_ARCH}" = "x64" ]]; then
            export CMAKE_ARGUMENTS="-DCMAKE_OSX_ARCHITECTURES=x86_64"
        elif [[ "${VCPKG_ARCH}" = "arm64" ]]; then
            export CMAKE_ARGUMENTS="-DCMAKE_OSX_ARCHITECTURES=arm64"
        else
            echo "Unknown architecture: ${VCPKG_ARCH}"
            exit 1
        fi
    fi

    echo "=== Setup VCPKG ==="

    pushd "${VCPKG_ROOT}"
    # XXX: patch an odd issue where the path of some file is inconsistent between builds
    patch -N -p1 < "${source_dir}/ci/vcpkg/0001-Work-around-inconsistent-path.patch" || true

    # XXX: make vcpkg retry downloads https://github.com/microsoft/vcpkg/discussions/20583
    patch -N -p1 < "${source_dir}/ci/vcpkg/0002-Retry-downloads.patch" || true
    popd

    # Need to install sqlite3 to make CMake be able to find it below
    "${VCPKG_ROOT}/vcpkg" install sqlite3 \
          --overlay-triplets "${VCPKG_OVERLAY_TRIPLETS}" \
          --triplet "${VCPKG_DEFAULT_TRIPLET}"

    "${VCPKG_ROOT}/vcpkg" install libpq \
          --overlay-triplets "${VCPKG_OVERLAY_TRIPLETS}" \
          --triplet "${VCPKG_DEFAULT_TRIPLET}"

    echo "=== Building drivers ==="
    mkdir -p ${build_dir}
    pushd ${build_dir}
    cmake \
        -DADBC_BUILD_SHARED=ON \
        -DADBC_BUILD_STATIC=ON \
        -DCMAKE_BUILD_WITH_INSTALL_RPATH=ON \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX=${build_dir} \
        -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
        -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
        ${CMAKE_ARGUMENTS} \
        -DVCPKG_OVERLAY_TRIPLETS="${VCPKG_OVERLAY_TRIPLETS}" \
        -DVCPKG_TARGET_TRIPLET="${VCPKG_DEFAULT_TRIPLET}" \
        -DADBC_DRIVER_BIGQUERY=ON \
        -DADBC_DRIVER_FLIGHTSQL=ON \
        -DADBC_DRIVER_MANAGER=ON \
        -DADBC_DRIVER_POSTGRESQL=ON \
        -DADBC_DRIVER_SQLITE=ON \
        -DADBC_DRIVER_SNOWFLAKE=ON \
        ${source_dir}/c
    cmake --build . --target install --verbose -j
    popd
}

function setup_build_vars {
    local -r arch="${1}"
    if [[ "$(uname)" = "Darwin" ]]; then
        if [[ "${arch}" = "amd64" ]]; then
            export CIBW_ARCHS="x86_64"
            export PYTHON_ARCH="x86_64"
            export VCPKG_ARCH="x64"
        elif [[ "${arch}" = "arm64v8" ]]; then
            export CIBW_ARCHS="arm64"
            export PYTHON_ARCH="arm64"
            export VCPKG_ARCH="arm64"
        else
            echo "Unknown architecture: ${arch}"
            exit 1
        fi
        export CIBW_BUILD='*-macosx_*'
        export CIBW_PLATFORM="macos"
    else
        if [[ "${arch}" = "amd64" ]]; then
            export CIBW_ARCHS="x86_64"
            export PYTHON_ARCH="x86_64"
            export VCPKG_ARCH="x64"
        elif [[ "${arch}" = "arm64v8" ]]; then
            export CIBW_ARCHS="aarch64"
            export PYTHON_ARCH="arm64"
            export VCPKG_ARCH="arm64"
        else
            echo "Unknown architecture: ${arch}"
            exit 1
        fi
        export CIBW_BUILD='*-manylinux_*'
        export CIBW_PLATFORM="linux"
    fi
    # No PyPy, no Python 3.8
    export CIBW_SKIP="pp* cp38-* ${CIBW_SKIP}"
}

function test_packages {
    for component in ${COMPONENTS}; do
        echo "=== Testing $component ==="

        python -c "
import $component
import $component.dbapi
"

        # --import-mode required, else tries to import from the source dir instead of installed package
        if [[ "${component}" = "adbc_driver_manager" ]]; then
            export PYTEST_ADDOPTS="${PYTEST_ADDOPTS} -k 'not duckdb and not sqlite'"
        fi
        python -m pytest -vvx --import-mode append ${source_dir}/python/$component/tests
    done
}

function test_packages_pyarrowless {
    local -r driver_path=$(python -c "import os; import adbc_driver_sqlite; print(os.path.dirname(adbc_driver_sqlite._driver_path()))")
    export LD_LIBRARY_PATH="${LD_LIBRARY_PATH}:${driver_path}"
    export DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}:${driver_path}"
    # For macOS (because we name the file ".so" on every platform regardless of the actual type)
    ln -s "${driver_path}/libadbc_driver_sqlite.so" "${driver_path}/libadbc_driver_sqlite.dylib"
    for component in ${COMPONENTS}; do
        echo "=== Testing $component (no PyArrow) ==="

        python -c "
import $component
import $component.dbapi
"

        local test_files=$(find ${source_dir}/python/$component/tests -type f |
                               grep -e 'nopyarrow\.py$')
        if [[ -z "${test_files}" ]]; then
            continue
        fi

        # --import-mode required, else tries to import from the source dir instead of installed package
        # set env var so that we don't skip tests if we somehow accidentally installed pyarrow
        env ADBC_NO_SKIP_TESTS=1 python -m pytest -vvx --import-mode append "${test_files[@]}"
    done
}
