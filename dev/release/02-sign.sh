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

set -eu

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
    if [ "$#" -ne 1 ]; then
        echo "Usage: $0 <rc-num>"
        echo "Usage: $0 0"
        exit 1
    fi

    pushd "${SOURCE_TOP_DIR}"

    local -r rc_number="$1"
    local -r tag="apache-arrow-adbc-${RELEASE}-rc${rc_number}"

    header "Looking for GitHub Actions workflow on ${REPOSITORY}:${tag}"
    local run_id=""
    while [[ -z "${run_id}" ]]
    do
        echo "Waiting for run to start..."
        run_id=$(gh run list \
                    --repo "${REPOSITORY}" \
                    --workflow=packaging.yml \
                    --json 'databaseId,event,headBranch,status' \
                    --jq ".[] | select(.event == \"push\" and .headBranch == \"${tag}\") | .databaseId")
        sleep 1
    done

    header "Found GitHub Actions workflow with ID: ${run_id}"
    gh run watch --repo "${REPOSITORY}" --exit-status ${run_id}
    gh run view --repo "${REPOSITORY}" "${run_id}"

    header "Downloading assets from release"
    local -r download_dir="packages/${tag}"
    mkdir -p "${download_dir}"
    gh release download \
       "${tag}" \
       --repo "${REPOSITORY}" \
       --dir "${download_dir}" \
       --skip-existing

    header "Adding release notes"
    local -r release_notes=$(changelog)
    echo "${release_notes}"
    gh release edit \
       "${tag}" \
       --repo "${REPOSITORY}" \
       --notes "${release_notes}"

    header "Upload signatures for source"
    upload_asset_signatures "${tag}" $(find "${download_dir}" -type f \( -name 'apache-arrow-adbc-*.tar.gz' \))

    header "Upload signatures for Java"
    upload_asset_signatures "${tag}" $(find "${download_dir}" -type f \( -name '*.jar' -or -name '*.pom' \))

    header "Upload signatures for Linux packages"
    upload_asset_signatures "${tag}" $(find "${download_dir}" -type f \( -name 'almalinux-*.tar.gz' -or -name 'debian-*.tar.gz' -or -name 'ubuntu-*.tar.gz' \))

    header "Upload signatures for Python"
    upload_asset_signatures "${tag}" $(find "${download_dir}" -type f \( -name '*.whl' -or -name 'adbc_*.tar.gz' \))

    header "Upload signatures for docs"
    upload_asset_signatures "${tag}" "${download_dir}/docs.tgz"

    popd
}

sign_asset() {
    local -r asset="$1"
    local -r sigfile="${asset}.asc"

    if [[ -f "${sigfile}" ]]; then
        if env LANG=C gpg --verify "${sigfile}" "${asset}" >/dev/null 2>&1; then
            echo "Valid signature at $(basename "${sigfile}"), skipping"
            return
        fi
        rm "${sigfile}"
    fi

    gpg \
        --armor \
        --detach-sign \
        --local-user "${GPG_KEY_ID}" \
        --output "${sigfile}" \
        "${asset}"
    echo "Generated $(basename "${sigfile}")"
}

sum_asset() {
    local -r asset="$1"
    local -r sumfile="${asset}.sha512"

    local -r digest=$(cd $(dirname "${asset}"); shasum --algorithm 512 $(basename "${asset}"))
    if [[ -f "${sumfile}" ]]; then
        if [[ "${digest}" = $(cat "${sumfile}") ]]; then
            echo "Valid digest at $(basename "${sumfile}"), skipping"
            return
        fi
    fi

    echo "${digest}" > "${sumfile}"
    echo "Generated $(basename "${sumfile}")"
}

upload_asset_signatures() {
    local -r tag="${1}"
    shift 1

    local -r assets=("$@")

    for asset in "${assets[@]}"; do
        sign_asset "${asset}"
        sum_asset "${asset}"
    done

    gh release upload \
       --repo "${REPOSITORY}" \
       "${tag}" \
       "${assets[@]/%/.asc}" \
       "${assets[@]/%/.sha512}"
}

main "$@"
