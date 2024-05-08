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
    if [ $# -ne 2 ]; then
        echo "Usage: $0 <arrow-dir> <rc-number>"
        echo "Usage: $0 ../arrow 0"
        exit
    fi

    local -r arrow_dir="$(cd "$1" && pwd)"
    local -r rc_number="$2"
    local -r tag="apache-arrow-adbc-${RELEASE}-rc${rc_number}"

    export ARROW_ARTIFACTS_DIR="$(pwd)/packages/${tag}/java"
    rm -rf "${ARROW_ARTIFACTS_DIR}"
    mkdir -p "${ARROW_ARTIFACTS_DIR}"
    gh release download \
       --dir "${ARROW_ARTIFACTS_DIR}" \
       --pattern "*.jar" \
       --pattern "*.jar.asc" \
       --pattern "*.pom" \
       --pattern "*.pom.asc" \
       --repo "${REPOSITORY}" \
       "${tag}"

    export UPLOAD_FORCE_SIGN=0
    "${arrow_dir}/dev/release/06-java-upload.sh" "${RELEASE}" "${rc_number}"
}

main "$@"
