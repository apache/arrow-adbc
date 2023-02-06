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

# Remove all uploaded packages. Only the latest nightly build will be
# accessible.

set -e
set -o pipefail
set -u

: ${CHANNEL:="arrow-adbc-nightlies"}
: ${API_BASE_URL:="https://api.anaconda.org"}

main() {
    local -r authorization="Authorization: token ${ANACONDA_API_TOKEN}"
    local packages
    local files
    packages=$(curl -s --fail -H "${authorization}" "${API_BASE_URL}/packages/${CHANNEL}" |
            jq -r '.[] | .name')

    for package in ${packages}; do
        echo "Cleaning ${package}"

        files=$(curl -s --fail -H "${authorization}" "${API_BASE_URL}/package/${CHANNEL}/${package}/files" | jq -r '.[] | .full_name')

        for file in ${files}; do
            echo "Deleting ${file}"

            curl -XDELETE -s --fail -H "${authorization}" "${API_BASE_URL}/dist/${file}"
        done
    done
}

main "$@"
