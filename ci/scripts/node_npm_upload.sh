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
# For non-npmjs registries (e.g. Gemfury nightlies), the script appends a
# nightly date suffix to the version (e.g. 0.23.0-nightly.20260330) to avoid
# "version already exists" errors.
#
# Usage: ./ci/scripts/node_npm_upload.sh <packages_dir>
#
# Environment variables:
#   NPM_TOKEN           npm authentication token (required)
#   NPM_REGISTRY        registry URL (default: https://registry.npmjs.org)
#   NPM_TAG             dist-tag to publish under (default: latest)
#   DRY_RUN             set to 1 to pass --dry-run to npm publish

set -euo pipefail

# Prevent macOS tar from including ._* resource fork files
export COPYFILE_DISABLE=1

# Rewrite the "version" field in a tarball's package.json and repack it
repack_with_version() {
    local pkg="$1" new_version="$2"
    local tmpdir
    tmpdir=$(mktemp -d)
    tar -xzf "${pkg}" -C "${tmpdir}"
    sed -i.bak "s/\"version\": *\"[^\"]*\"/\"version\": \"${new_version}\"/" "${tmpdir}/package/package.json"
    rm -f "${tmpdir}/package/package.json.bak"
    tar -czf "${pkg}" -C "${tmpdir}" package
    rm -rf "${tmpdir}"
}

# Read the "version" field from a tarball's package.json
read_tarball_version() {
    tar -xzf "$1" -O package/package.json | grep -o '"version": *"[^"]*"' | head -1 | cut -d'"' -f4
}

main() {
    local packages_dir
    packages_dir="$(realpath "$1")"
    local registry="${NPM_REGISTRY:-https://registry.npmjs.org}"
    local registry_key="${registry%/}"
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

    local npmrc
    npmrc=$(mktemp)
    trap "rm -f ${npmrc}" EXIT
    echo "//${registry_key#*://}/:_authToken=${NPM_TOKEN}" > "${npmrc}"

    # For non-npmjs registries (e.g. Gemfury), append a nightly date suffix
    # so each publish gets a unique version
    if [[ "${registry}" != *"registry.npmjs.org"* ]]; then
        local base_version
        base_version=$(read_tarball_version "$(set -- "${packages_dir}"/apache-arrow-adbc-driver-manager-[0-9]*.tgz; echo "$1")")
        local new_version="${base_version}-nightly.$(date +%Y%m%d)"
        echo "==== Rewriting version to ${new_version} for nightly publish"
        for pkg in "${packages_dir}"/apache-arrow-adbc-driver-manager-*.tgz; do
            repack_with_version "${pkg}" "${new_version}"
        done
    fi

    # Publish platform-specific packages first, then the main package
    local platform_pkgs=()
    shopt -s nullglob
    platform_pkgs=("${packages_dir}"/apache-arrow-adbc-driver-manager-*-*.tgz)
    shopt -u nullglob
    for pkg in "${platform_pkgs[@]+"${platform_pkgs[@]}"}"; do
        echo "==== Publishing ${pkg}"
        npm publish "${pkg}" --access public --registry "${registry}" --userconfig "${npmrc}" ${tag_flag} ${dry_run_flag}
    done

    echo "==== Publishing main package"
    local main_pkg
    main_pkg=$(set -- "${packages_dir}"/apache-arrow-adbc-driver-manager-[0-9]*.tgz; echo "$1")
    npm publish "${main_pkg}" --access public --registry "${registry}" --userconfig "${npmrc}" ${tag_flag} ${dry_run_flag}
}

main "$@"
