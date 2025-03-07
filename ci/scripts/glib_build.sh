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

build_subproject() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local -r install_dir="${3}"

    local cmake_prefix_path="${install_dir}"
    local pkg_config_path="${install_dir}/lib/pkgconfig"
    if [[ -n "${CMAKE_PREFIX_PATH}" ]]; then
        cmake_prefix_path="${cmake_prefix_path}:${CMAKE_PREFIX_PATH}"
    fi
    if [[ -n "${PKG_CONFIG_PATH}" ]]; then
        pkg_config_path="${pkg_config_path}:${PKG_CONFIG_PATH}"
    fi
    if [[ -n "${CONDA_PREFIX}" ]]; then
        cmake_prefix_path="${CONDA_PREFIX}:${cmake_prefix_path}"
        pkg_config_path="${pkg_config_path}:${CONDA_PREFIX}/lib/pkgconfig"
    fi
    if type valac > /dev/null 2>&1 && \
            [[ -n "$(pkg-config --variable=vapidir arrow-glib)" ]]; then
        enable_vapi=true
    else
        enable_vapi=false
    fi

    meson setup \
          --buildtype=debug \
          --cmake-prefix-path="${cmake_prefix_path}" \
          --libdir=lib \
          --pkg-config-path="${pkg_config_path}" \
          --prefix="${install_dir}" \
          -Dexample=true \
          -Dvapi="${enable_vapi}" \
          "${build_dir}/glib" \
          "${source_dir}/glib"
    meson install -C "${build_dir}/glib"

    export DYLD_LIBRARY_PATH="${install_dir}/lib${DYLD_LIBRARY_PATH:+:${DYLD_LIBRARY_PATH}}"
    export LD_LIBRARY_PATH="${install_dir}/lib${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
    "${build_dir}/glib/example/sqlite"
    if [[ "${enable_vapi}" = "true" ]]; then
        "${build_dir}/glib/example/vala/sqlite"
    fi
}

main() {
    local -r source_dir="${1}"
    local -r build_dir="${2}"
    local install_dir="${3}"

    if [[ -z "${install_dir}" ]]; then
        install_dir="${build_dir}/local"
    fi

    if [[ "${BUILD_DRIVER_MANAGER}" -gt 0 ]]; then
        build_subproject "${source_dir}" "${build_dir}" "${install_dir}" adbc_driver_manager
    fi
}

main "$@"
