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

arch=${1}
source_dir=${2}
build_dir=${3}

function check_visibility {
    if [[ $(uname) != "Linux" ]]; then
       return 0
    fi
    nm --demangle --dynamic $1 > nm_arrow.log

    # Filter out Arrow symbols and see if anything remains.
    # '_init' and '_fini' symbols may or not be present, we don't care.
    # (note we must ignore the grep exit status when no match is found)
    grep ' T ' nm_arrow.log | grep -v -E '(Adbc|\b_init\b|\b_fini\b)' | cat - > visible_symbols.log

    if [[ -f visible_symbols.log && `cat visible_symbols.log | wc -l` -eq 0 ]]; then
        return 0
    else
        echo "== Unexpected symbols exported by $1 =="
        cat visible_symbols.log
        echo "================================================"

        exit 1
    fi
}

function check_wheels {
    if [[ $(uname) == "Linux" ]]; then
        echo "=== (${PYTHON_VERSION}) Tag $component wheel with manylinux${MANYLINUX_VERSION} ==="
        auditwheel repair "$@" -L . -w repaired_wheels
    else # macOS
        echo "=== (${PYTHON_VERSION}) Check $component wheel for unbundled dependencies ==="
        local -r deps=$(delocate-listdeps dist/$component-*.whl)
        if ! echo $deps | grep -v "python/"; then
            echo "There are unbundled dependencies."
            exit 1
        fi
    fi
}

echo "=== (${PYTHON_VERSION}) Building ADBC libpq driver ==="
: ${CMAKE_BUILD_TYPE:=release}
: ${CMAKE_UNITY_BUILD:=ON}
: ${CMAKE_GENERATOR:=Ninja}
: ${VCPKG_ROOT:=/opt/vcpkg}
# Enable manifest mode
: ${VCPKG_FEATURE_FLAGS:=manifests}
# Add our custom triplets
: ${VCPKG_OVERLAY_TRIPLETS:="${source_dir}/ci/vcpkg/triplets/"}

if [[ $(uname) == "Linux" ]]; then
    export ADBC_POSTGRES_LIBRARY=${build_dir}/lib/libadbc_driver_postgres.so
    : ${VCPKG_DEFAULT_TRIPLET:="x64-linux-static-release"}
else # macOS
    export ADBC_POSTGRES_LIBRARY=${build_dir}/lib/libadbc_driver_postgres.dylib
    : ${VCPKG_DEFAULT_TRIPLET:="x64-osx-static-release"}
fi

mkdir -p ${build_dir}
pushd ${build_dir}

cmake \
    -G ${CMAKE_GENERATOR} \
    -DADBC_BUILD_SHARED=ON \
    -DADBC_BUILD_STATIC=OFF \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=${build_dir} \
    -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
    -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
    ${source_dir}/c/driver/postgres
cmake --build . --target install -j
popd

# Check that we don't expose any unwanted symbols
check_visibility $ADBC_POSTGRES_LIBRARY

# https://github.com/pypa/pip/issues/7555
# Get the latest pip so we have in-tree-build by default
pip install --upgrade pip

for component in adbc_driver_manager adbc_driver_postgres; do
    pushd ${source_dir}/python/$component

    echo "=== (${PYTHON_VERSION}) Clean build artifacts==="
    rm -rf ./build ./dist ./repaired_wheels ./$component/*.so ./$component/*.so.*

    echo "=== (${PYTHON_VERSION}) Building $component wheel ==="
    # python -m build copies to a tempdir, so we can't reference other files in the repo
    # https://github.com/pypa/pip/issues/5519
    python -m pip wheel -w dist -vvv .

    check_wheels dist/$component-*.whl

    popd
done
