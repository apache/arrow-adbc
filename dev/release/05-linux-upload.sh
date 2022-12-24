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
set -u
set -o pipefail

main() {
    local -r source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local -r source_top_dir="$(cd "${source_dir}/../../" && pwd)"

    if [ $# -ne 3 ]; then
        echo "Usage: $0 <arrow-dir> <version> <rc-number>"
        echo "Usage: $0 ../arrow 1.0.0 0"
        exit
    fi

    local -r arrow_dir="$(cd "$1" && pwd)"
    local -r version="$2"
    local -r rc_number="$3"
    local -r tag="apache-arrow-adbc-${version}-rc${rc_number}"

    : ${REPOSITORY:="apache/arrow-adbc"}

    export ARROW_ARTIFACTS_DIR="$(pwd)/packages/${tag}/linux"
    rm -rf "${ARROW_ARTIFACTS_DIR}"
    mkdir -p "${ARROW_ARTIFACTS_DIR}"
    gh release download \
       --dir "${ARROW_ARTIFACTS_DIR}" \
       --pattern "almalinux-*.tar.gz" \
       --pattern "debian-*.tar.gz" \
       --pattern "ubuntu-*.tar.gz" \
       --repo "${REPOSITORY}" \
       "${tag}"

    pushd "${ARROW_ARTIFACTS_DIR}"
    local base_dir=
    for tar_gz in *.tar.gz; do
        mkdir -p tmp
        tar xf ${tar_gz} -C tmp
        base_dir=${tar_gz%.tar.gz}
        mkdir -p ${base_dir}
        find tmp -type f -print0 | xargs -0 mv -t ${base_dir}
        rm -rf tmp
	rm -f ${tar_gz}
    done
    popd

    export DEB_PACKAGE_NAME=apache-arrow-adbc
    export UPLOAD_DEFAULT=0
    export UPLOAD_ALMALINUX=${UPLOAD_ALMALINUX:-1}
    export UPLOAD_DEBIAN=${UPLOAD_DEBIAN:-1}
    export UPLOAD_UBUNTU=${UPLOAD_UBUNTU:-1}
    "${arrow_dir}/dev/release/05-binary-upload.sh" "${version}" "${rc_number}"
}

main "$@"
