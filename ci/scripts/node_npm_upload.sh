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

# Publish Node.js packages to an npm registry.
#
# Usage: ./ci/scripts/node_npm_upload.sh <packages_dir>
#
# Environment variables:
#   NPM_TOKEN        npm authentication token (required)
#   NPM_REGISTRY     registry URL (default: https://registry.npmjs.org)
#   NPM_TAG          dist-tag to publish under (default: latest)
#   DRY_RUN          set to 1 to pass --dry-run to npm publish

set -euo pipefail

main() {
    local packages_dir
    packages_dir="$(realpath "$1")"
    local registry="${NPM_REGISTRY:-https://registry.npmjs.org}"
    local registry_key="${registry%/}"  # strip trailing slash for .npmrc key construction
    local dry_run_flag=""
    if [[ "${DRY_RUN:-0}" == "1" ]]; then
        dry_run_flag="--dry-run"
    fi
    local tag_flag=""
    if [[ -n "${NPM_TAG:-}" ]]; then
        tag_flag="--tag ${NPM_TAG}"
    fi

    if [[ -z "${NPM_TOKEN:-}" ]]; then
        echo "Error: NPM_TOKEN is required" >&2
        exit 1
    fi

    # Write a temp .npmrc with the auth token for the target registry
    local npmrc
    npmrc=$(mktemp)
    trap "rm -f ${npmrc}" EXIT
    echo "//${registry_key#*://}/:_authToken=${NPM_TOKEN}" > "${npmrc}"

    # Publish platform-specific packages first, then the main package
    for pkg in "${packages_dir}"/apache-arrow-adbc-driver-manager-*-*.tgz; do
        echo "==== Publishing ${pkg}"
        npm publish "${pkg}" --access public --registry "${registry}" --userconfig "${npmrc}" ${tag_flag} ${dry_run_flag}
    done

    echo "==== Publishing main package"
    npm publish "${packages_dir}"/apache-arrow-adbc-driver-manager-[0-9]*.tgz \
        --access public --registry "${registry}" --userconfig "${npmrc}" ${tag_flag} ${dry_run_flag}
}

main "$@"
