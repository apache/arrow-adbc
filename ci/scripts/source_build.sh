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

set -euo pipefail
set -x

main() {
    local -r source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local -r source_top_dir="$(cd "${source_dir}/../../" && pwd)"

    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <base-name> <revision>"
        echo "Usage: $0 apache-arrow-adbc-1.0.0 1234567"
        exit 1
    fi
    local -r base_name="$1"
    local -r revision="$2"

    echo "Using commit ${revision}"

    local -r tar_ball="${base_name}.tar.gz"

    pushd "${source_top_dir}"

    rm -rf "${base_name}/"
    git archive "${revision}" --prefix "${base_name}/" | tar xf -

    # Resolve all submodules
    while read SUBMODULE; do
        SUBMODULE_REV=$(echo "${SUBMODULE}" | awk '{print $1}')
        SUBMODULE_PATH=$(echo "${SUBMODULE}" | awk '{print $2}')
        git -C "${SUBMODULE_PATH}" archive --prefix="${base_name}/${SUBMODULE_PATH}/" "${SUBMODULE_REV}" | tar xf - -C "${source_top_dir}"
    done < <(git submodule status)

    # Resolve all hard and symbolic links
    rm -rf "${base_name}.tmp/"
    mv "${base_name}/" "${base_name}.tmp/"
    cp -R -L "${base_name}.tmp" "${base_name}"
    rm -rf "${base_name}.tmp/"

    # Create new tarball
    tar czf "${tar_ball}" "${base_name}/"
    rm -rf "${base_name}/"

    # check licenses
    "${source_top_dir}/dev/release/run-rat.sh" "${tar_ball}"

    echo "Commit SHA1: ${revision}"

    popd
}

main "$@"
