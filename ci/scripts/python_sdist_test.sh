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

set -e
set -x
set -o pipefail

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <adbc-src-dir>"
  exit 1
fi

COMPONENTS="adbc_driver_manager adbc_driver_postgres"

source_dir=${1}
build_dir="${source_dir}/build"

# TODO(lidavidm): reduce duplication between wheel build/sdist test

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
    export VCPKG_DEFAULT_TRIPLET="x64-linux-static-release"
else # macOS
    export ADBC_POSTGRES_LIBRARY=${build_dir}/lib/libadbc_driver_postgres.dylib
    export VCPKG_DEFAULT_TRIPLET="x64-osx-static-release"
fi

echo ${VCPKG_DEFAULT_TRIPLET}

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

echo "=== (${PYTHON_VERSION}) Installing sdists ==="
for component in ${COMPONENTS}; do
    pip install --force-reinstall ${source_dir}/python/${component}/dist/*.tar.gz
done
pip install pytest pyarrow pandas

echo "=== (${PYTHON_VERSION}) Testing sdists ==="
python -c "
import adbc_driver_manager
import adbc_driver_manager.dbapi
import adbc_driver_postgres
import adbc_driver_postgres.dbapi
"

# Will only run some smoke tests
# --import-mode required, else tries to import from the source dir instead of installed package
echo "=== Testing adbc_driver_manager ==="
python -m pytest -vvx --import-mode append -k "not sqlite" ${source_dir}/python/adbc_driver_manager/tests
echo "=== Testing adbc_driver_postgres ==="
python -m pytest -vvx --import-mode append ${source_dir}/python/adbc_driver_postgres/tests
