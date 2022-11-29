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

COMPONENTS="adbc_driver_manager adbc_driver_postgres adbc_driver_sqlite"

function build_drivers {
    local -r source_dir="$1"
    local -r build_dir="$2"

    : ${CMAKE_BUILD_TYPE:=release}
    : ${CMAKE_UNITY_BUILD:=ON}
    : ${CMAKE_GENERATOR:=Ninja}
    : ${VCPKG_ROOT:=/opt/vcpkg}
    # Enable manifest mode
    : ${VCPKG_FEATURE_FLAGS:=manifests}
    # Add our custom triplets
    export VCPKG_OVERLAY_TRIPLETS="${source_dir}/ci/vcpkg/triplets/"

    if [[ $(uname) == "Linux" ]]; then
        export ADBC_POSTGRES_LIBRARY=${build_dir}/lib/libadbc_driver_postgres.so
        export ADBC_SQLITE_LIBRARY=${build_dir}/lib/libadbc_driver_sqlite.so
        export VCPKG_DEFAULT_TRIPLET="x64-linux-static-release"
    else # macOS
        export ADBC_POSTGRES_LIBRARY=${build_dir}/lib/libadbc_driver_postgres.dylib
        export ADBC_SQLITE_LIBRARY=${build_dir}/lib/libadbc_driver_sqlite.dylib
        export VCPKG_DEFAULT_TRIPLET="x64-osx-static-release"
    fi

    echo "=== Building driver/postgres ==="
    mkdir -p ${build_dir}/driver/postgres
    pushd ${build_dir}/driver/postgres
    cmake \
        -G ${CMAKE_GENERATOR} \
        -DADBC_BUILD_SHARED=ON \
        -DADBC_BUILD_STATIC=OFF \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX=${build_dir} \
        -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
        -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
        -DVCPKG_OVERLAY_TRIPLETS="${VCPKG_OVERLAY_TRIPLETS}" \
        -DVCPKG_TARGET_TRIPLET="${VCPKG_DEFAULT_TRIPLET}" \
        ${source_dir}/c/driver/postgres
    cmake --build . --target install -j
    popd

    echo "=== Building driver/sqlite ==="
    mkdir -p ${build_dir}/driver/sqlite
    pushd ${build_dir}/driver/sqlite
    cmake \
        -G ${CMAKE_GENERATOR} \
        -DADBC_BUILD_SHARED=ON \
        -DADBC_BUILD_STATIC=OFF \
        -DCMAKE_INSTALL_LIBDIR=lib \
        -DCMAKE_INSTALL_PREFIX=${build_dir} \
        -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
        -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
        -DVCPKG_OVERLAY_TRIPLETS="${VCPKG_OVERLAY_TRIPLETS}" \
        -DVCPKG_TARGET_TRIPLET="${VCPKG_DEFAULT_TRIPLET}" \
        ${source_dir}/c/driver/sqlite
    cmake --build . --target install -j
    popd
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
            python -m pytest -vvx --import-mode append -k "not sqlite" ${source_dir}/python/$component/tests
        else
            python -m pytest -vvx --import-mode append ${source_dir}/python/$component/tests
        fi
    done
}
