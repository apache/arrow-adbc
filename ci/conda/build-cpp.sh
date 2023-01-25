#!/bin/bash
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

case "${PKG_NAME}" in
    adbc-driver-manager-cpp)
        export PKG_ROOT=c/driver_manager
        ;;
    adbc-driver-flightsql-go)
        export CGO_ENABLED=1
        export PKG_ROOT=c/driver/flightsql
        ;;
    adbc-driver-postgresql-cpp)
        export PKG_ROOT=c/driver/postgresql
        ;;
    adbc-driver-sqlite-cpp)
        export PKG_ROOT=c/driver/sqlite
        ;;
    *)
        echo "Unknown package ${PKG_NAME}"
        exit 1
        ;;
esac

if [[ "${target_platform}" == "linux-aarch64" ]] ||
       [[ "${target_platform}" == "osx-arm64" ]]; then
    export GOARCH="arm64"
elif [[ "${target_platform}" == "linux-ppc64le" ]]; then
    export GOARCH="ppc64le"
else
    export GOARCH="amd64"
fi

mkdir -p "build-cpp/${PKG_NAME}"
pushd "build-cpp/${PKG_NAME}"

cmake "../../${PKG_ROOT}" \
      -G Ninja \
      -DADBC_BUILD_SHARED=ON \
      -DADBC_BUILD_STATIC=OFF \
      -DCMAKE_INSTALL_PREFIX="${PREFIX}" \
      -DCMAKE_PREFIX_PATH="${PREFIX}"

cmake --build . --target install -j

popd
