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

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <arrow-dir> <rc-num>"
        echo "Usage: $0 ../arrow 0"
        exit 1
    fi

    local -r arrow_dir="$(cd "$1" && pwd)"
    local -r rc_number="$2"

    header "Deploying APT/Yum repositories ${RELEASE}"

    export DEPLOY_DEFAULT=0
    export DEPLOY_ALMALINUX=${DEPLOY_ALMALINUX:-1}
    export DEPLOY_DEBIAN=${DEPLOY_DEBIAN:-1}
    export DEPLOY_UBUNTU=${DEPLOY_UBUNTU:-1}
    "${arrow_dir}/dev/release/post-02-binary.sh" "${RELEASE}" "${rc_number}"
}

main "$@"
