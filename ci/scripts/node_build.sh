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

# Usage: ./node_build.sh <out_dir>
if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <out_dir>"
  exit 1
fi

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
OUT_DIR="$1"

# 1. Build the C++ Drivers
echo "=== Building C++ Drivers ==="
mkdir -p "${OUT_DIR}"

cmake -S "${REPO_ROOT}/c" -B "${OUT_DIR}" \
  ${ADBC_CMAKE_ARGS} \
  -DCMAKE_INSTALL_PREFIX="${OUT_DIR}" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DADBC_DRIVER_SQLITE=ON \
  -DADBC_BUILD_TESTS=OFF \
  -DADBC_BUILD_SHARED=ON \
  -DADBC_BUILD_STATIC=OFF

cmake --build "${OUT_DIR}" --config RelWithDebInfo --target install -j

# 2. Build Node.js Package
echo "=== Building Node.js Package ==="
pushd "${REPO_ROOT}/javascript"

npm install
npm run build

popd
