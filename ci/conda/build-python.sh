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

if [[ "$(uname)" = "Darwin" ]]; then
    LIB_SUFFIX="dylib"
else
    LIB_SUFFIX="so"
fi

if [[ "${PKG_NAME}" = "adbc-driver-manager" ]]; then
    pushd python/adbc_driver_manager
elif [[ "${PKG_NAME}" = "adbc-driver-flightsql" ]]; then
    pushd python/adbc_driver_flightsql
    export ADBC_FLIGHTSQL_LIBRARY=$PREFIX/lib/libadbc_driver_flightsql.$LIB_SUFFIX
elif [[ "${PKG_NAME}" = "adbc-driver-postgresql" ]]; then
    pushd python/adbc_driver_postgresql
    export ADBC_POSTGRESQL_LIBRARY=$PREFIX/lib/libadbc_driver_postgresql.$LIB_SUFFIX
elif [[ "${PKG_NAME}" = "adbc-driver-sqlite" ]]; then
    pushd python/adbc_driver_sqlite
    export ADBC_SQLITE_LIBRARY=$PREFIX/lib/libadbc_driver_sqlite.$LIB_SUFFIX
else
    echo "Unknown package ${PKG_NAME}"
    exit 1
fi

export _ADBC_IS_CONDA=1
export SETUPTOOLS_SCM_PRETEND_VERSION=$PKG_VERSION

echo "==== INSTALL ${PKG_NAME}"
$PYTHON -m pip install . -vvv --no-deps

popd
