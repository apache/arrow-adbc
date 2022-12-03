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
    local -r version="$1"
    local -r rc_number="$2"
    local -r tag="adbc-${version}-rc${rc_number}"

    : ${ADBC_REPOSITORY:="apache/arrow-adbc"}

    export ARROW_ARTIFACTS_DIR="$(pwd)/packages/${tag}/java"
    rm -rf "${ARROW_ARTIFACTS_DIR}"
    mkdir -p "${ARROW_ARTIFACTS_DIR}"
    gh release download \
       --dir "${ARROW_ARTIFACTS_DIR}" \
       --pattern "*.jar" \
       --pattern "*.jar.asc" \
       --pattern "*.pom" \
       --pattern "*.pom.asc" \
       --repo "${ADBC_REPOSITORY}" \
       "${tag}"

    export UPLOAD_FORCE_SIGN=0
    "${arrow_dir}/dev/release/06-java-upload.sh" "${version}" "${rc_number}"
}

main "$@"
