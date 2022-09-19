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

function check_visibility {
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

echo "=== (${PYTHON_VERSION}) Building ADBC libpq driver ==="
: ${CMAKE_BUILD_TYPE:=release}
: ${CMAKE_UNITY_BUILD:=ON}
: ${CMAKE_GENERATOR:=Ninja}
: ${VCPKG_ROOT:=/opt/vcpkg}

mkdir /tmp/libpq-build
pushd /tmp/libpq-build

cmake \
    -G ${CMAKE_GENERATOR} \
    -DADBC_BUILD_SHARED=ON \
    -DADBC_BUILD_STATIC=OFF \
    -DCMAKE_INSTALL_LIBDIR=lib \
    -DCMAKE_INSTALL_PREFIX=/tmp/libpq-dist \
    -DCMAKE_TOOLCHAIN_FILE=${VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake \
    -DCMAKE_UNITY_BUILD=${CMAKE_UNITY_BUILD} \
    /adbc/c/driver/postgres
cmake --build . --target install -j
popd

export ADBC_POSTGRES_LIBRARY=/tmp/libpq-dist/lib/libadbc_driver_postgres.so

# Check that we don't expose any unwanted symbols
check_visibility $ADBC_POSTGRES_LIBRARY

python -m pip install poetry

for component in adbc_driver_manager adbc_driver_postgres; do
    pushd /adbc/python/$component

    echo "=== (${PYTHON_VERSION}) Clean build artifacts==="
    rm -rf ./build ./dist ./repaired_wheels ./$component/*.so ./$component/*.so.*

    echo "=== (${PYTHON_VERSION}) Building $component wheel ==="
    python -m poetry build

    echo "=== (${PYTHON_VERSION}) Tag $component wheel with manylinux${MANYLINUX_VERSION} ==="
    auditwheel repair dist/$component-*.whl -L . -w repaired_wheels

    popd
done
