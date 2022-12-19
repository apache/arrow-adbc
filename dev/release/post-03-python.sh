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
    local -r source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local -r source_top_dir="$( cd "${source_dir}/../../" && pwd )"

    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <version> <rc-num>"
        exit 1
    fi

    local -r version="$1"
    local -r rc_number="$2"
    local -r tag="apache-arrow-adbc-${version}"

    : ${REPOSITORY:="apache/arrow-adbc"}

    local -r tmp=$(mktemp -d -t "arrow-post-python.XXXXX")

    header "Downloading Python packages for ${version}"

    gh release download \
       --repo "${REPOSITORY}" \
       "${tag}" \
       --dir "${tmp}" \
       --pattern "*.whl" \
       --pattern "adbc_*.tar.gz" # sdist

    header "Uploading Python packages for ${version}"

    TWINE_ARGS=""
    if [ "${TEST_PYPI:-0}" -gt 0 ]; then
        echo "Using test.pypi.org"
        TWINE_ARGS="${TWINE_ARGS} --repository-url https://test.pypi.org/legacy/"
    fi

    twine upload ${TWINE_ARGS} ${tmp}/*

    rm -rf "${tmp}"

    echo "Success!"
}

header() {
    echo "============================================================"
    echo "${1}"
    echo "============================================================"
}

main "$@"
