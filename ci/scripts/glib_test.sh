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

: ${BUILD_ALL:=1}
: ${BUILD_DRIVER_MANAGER:=${BUILD_ALL}}

test_subproject() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local -r install_dir="${3}"

    export DYLD_LIBRARY_PATH="${install_dir}/lib"
    export GI_TYPELIB_PATH="${build_dir}/glib/adbc-glib"
    export LD_LIBRARY_PATH="${install_dir}/lib"
    export PKG_CONFIG_PATH="${install_dir}/lib/pkgconfig"
    if [[ -n "${CONDA_PREFIX}" ]]; then
        export GI_TYPELIB_PATH="${GI_TYPELIB_PATH}:${CONDA_PREFIX}/lib/girepository-1.0"
        export PKG_CONFIG_PATH="${PKG_CONFIG_PATH}:${CONDA_PREFIX}/lib/pkgconfig"
    fi

    pushd "${source_dir}/glib"
    bundle install
    bundle exec \
           env DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}" \
           ruby "${source_dir}/glib/test/run.rb"
    popd

    export GI_TYPELIB_PATH="${install_dir}/lib/girepository-1.0:${GI_TYPELIB_PATH}"
    pushd "${source_dir}/ruby"
    bundle install
    bundle exec \
           env DYLD_LIBRARY_PATH="${DYLD_LIBRARY_PATH}" \
           ruby test/run.rb
    bundle exec rake build
    gem install --user-install pkg/*.gem
    popd
}

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local install_dir="${3}"

    if [[ -z "${install_dir}" ]]; then
        install_dir="${build_dir}/local"
    fi

    if [[ "${BUILD_DRIVER_MANAGER}" -gt 0 ]]; then
        test_subproject "${source_dir}" "${build_dir}" "${install_dir}" adbc_driver_manager
    fi
}

main "$@"
