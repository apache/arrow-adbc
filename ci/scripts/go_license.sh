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

# Helper to assemble LICENSE.txt for thirdparty Go dependencies.
# Since we ship wheels (binary artifacts) with statically compiled Go
# code, we need to make sure we respect the licenses.
# https://infra.apache.org/licensing-howto.html

set -e
set -o pipefail
set -u

main() {
    local -r source_dir="${1}"
    local -r output="${2}"

    pushd "${source_dir}/go/adbc"
    local -r arrow_pattern='^github\.com/apache/arrow'
    local -r github_pattern='^https://github\.com'
    local dependency
    local license_url
    local license_name

    go-licenses report ./... 2>/dev/null | while read -r license_line; do
        # Skip ourselves
        if [[ "${license_line}" =~ $arrow_pattern ]]; then
            continue
        fi

        dependency=$(echo "${license_line}" | awk -F, '{print $1}')
        license_url=$(echo "${license_line}" | awk -F, '{print $2}')
        license_name=$(echo "${license_line}" | awk -F, '{print $3}')

        {
            echo
            echo "--------------------------------------------------------------------------------"
            echo
            echo "3rdparty dependency ${dependency}"
            echo "is statically linked in certain binary distributions, like the Python wheels."
            echo "${dependency} is under the ${license_name} license."
        } >> "${output}"

        >&2 echo "${license_name} dependency: ${dependency}"
        if [[ "${license_name}" = "Apache-2.0" ]]; then
            # No need to bundle Apache license, but may need to analyze NOTICE.txt
            >&2 echo "*** Please analyze NOTICE.txt (${license_url})"
        else
            if [[ "${license_url}" =~ $github_pattern ]]; then
                # Fetch the license automatically
                license_url=$(echo "${license_url}" | sed 's|github.com|raw.githubusercontent.com|' | sed 's|blob/||')
                curl --silent --fail "${license_url}" >> "${output}"
            else
                >&2 echo "*** Please paste LICENSE into output (${license_url})"
            fi
        fi
    done
}

main "$@"
