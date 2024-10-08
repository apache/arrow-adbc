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

# Regenerate the versions.txt used to power the version switcher in
# the docs
# Assumes sphobjinv is installed (pip)

set -e
set -o pipefail
set -u
set -x

main() {
    if [[ "$#" != 3 ]]; then
       echo "Usage: ${0} <adbc-checkout> <asf-site-checkout> <new-docs-path>"
       exit 1
    fi
    # Path to ADBC repo
    local -r repo="${1}"
    # Path to ADBC repo, asf-site branch
    local -r site="${2}"
    # The path to the newly generated docs
    local -r docs="${3}"

    # Determine path for new docs

    local -r new_version=$(sphobjinv convert json "${docs}/objects.inv" - | jq -r .version)
    if [[ -z "${new_version}" ]]; then
        echo "Could not determine version of generated docs"
        exit 1
    fi

    # Docs use the ADBC release so it will just be 12, 13, 14, ...
    local -r regex='^[0-9]+$'
    local directory="main"
    if [[ "${new_version}" =~ $regex ]]; then
        echo "Adding docs for version ${new_version}"
        cp -r "${docs}" "${site}/${new_version}"
        git -C "${site}" add --force "${new_version}"
        directory="${new_version}"
    else
        # Assume this is dev docs
        echo "Adding dev docs for version ${new_version}"
        rm -rf "${site}/main"
        cp -r "${docs}" "${site}/main"
        git -C "${site}" add --force "main"
        directory="main"
    fi

    # Fix up lazy Intersphinx links (see docs_build.sh)
    # Assumes GNU sed
    sed -i "s|http://javadocs.home.arpa/|https://arrow.apache.org/adbc/${directory}/|g" $(grep -Rl javadocs.home.arpa "${site}/${directory}/")
    sed -i "s|http://doxygen.home.arpa/|https://arrow.apache.org/adbc/${directory}/|g" $(grep -Rl doxygen.home.arpa "${site}/${directory}/")
    git -C "${site}" add --force "${directory}"

    # Copy the version script and regenerate the version list
    # The versions get embedded into the JavaScript file to save a roundtrip
    rm -f "${site}/version.js"
    echo 'const versions = `' >> "${site}/version.js"

    pushd "${site}"
    rm -f "${site}/versions.txt"
    for inv in */objects.inv; do
        if [[ "$(dirname $inv)" = "current" ]]; then
            continue
        fi
        echo "$(dirname $inv);$(sphobjinv convert json $inv - | jq -r .version)"
    done | sort -t ";" --version-sort | tee --append "${site}/version.js" "${site}/versions.txt"
    popd

    # Determine the latest stable version
    local -r latest_docs=$(grep -E ';[0-9]+(\.[0-9]+\.[0-9]+)?$' "${site}/versions.txt" | sort -t ';' --version-sort | tail -n1)
    if [[ -z "${latest_docs}" ]]; then
        echo "No stable versions found"
        local -r latest_dir="main"
        local -r latest_version="${new_version}"
    else
        local -r latest_dir=$(echo "${latest_docs}" | awk -F';' '{print $1}')
        local -r latest_version=$(echo "${latest_docs}" | awk -F';' '{print $2}')
    fi
    echo "Latest version: ${latest_version} in directory ${latest_dir}"

    # Make a copy of the latest release under a stable URL
    rm -rf "${site}/current/"
    cp -r "${site}/${latest_dir}" "${site}/current/"
    git -C "${site}" add -f "current"

    echo "current;${latest_version} (current)" >> "${site}/version.js"

    echo '`;' >> "${site}/version.js"
    cat "${repo}/docs/source/_static/version.js" >> "${site}/version.js"
    git -C "${site}" add -f "version.js"

    # Generate index.html
    cat > "${site}/index.html" << EOF
<!DOCTYPE html>
<meta http-equiv="Refresh" content="0; url=current/index.html">
EOF
    git -C "${site}" add -f "index.html"

    # Generate latest/index.html
    mkdir -p "${site}/latest"
    cat > "${site}/latest/index.html" << EOF
<!DOCTYPE html>
<meta http-equiv="Refresh" content="0; url=../current/index.html">
EOF
    git -C "${site}" add -f "latest/index.html"
}

main "$@"
