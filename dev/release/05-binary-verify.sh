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

set -euo pipefail

main() {
    local -r source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local -r source_top_dir="$( cd "${source_dir}/../../" && pwd )"

    local -r version="$1"
    local -r rc_number="$2"
    local -r tag="apache-arrow-adbc-${version}-rc${rc_number}"

    : ${REPOSITORY:="apache/arrow-adbc"}

    echo "Starting GitHub Actions workflow on ${REPOSITORY} for ${version} RC${rc_number}"

    gh workflow run \
       --repo "${REPOSITORY}" \
       --ref "${tag}" \
       verify.yml \
       --raw-field version="${version}" \
       --raw-field rc="${rc_number}"

    local run_id=""
    while [[ -z "${run_id}" ]]
    do
        echo "Waiting for run to start..."
        run_id=$(gh run list \
                    --repo "${REPOSITORY}" \
                    --workflow=verify.yml \
                    --json 'databaseId,event,headBranch,status' \
                    --jq ".[] | select(.event == \"workflow_dispatch\" and .headBranch == \"${tag}\" and .status != \"completed\") | .databaseId")
        sleep 1
    done

    echo "Started GitHub Actions workflow with ID: ${run_id}"
    echo "You can wait for completion via: gh run watch --repo ${REPOSITORY} ${run_id}"
}

main "$@"
