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

: ${DRY_RUN:=0}
: ${REPOSITORY:="apache/arrow-adbc"}
: ${WORKFLOW_REF:="main"}

SOURCE_TOP_DIR="$( cd "${SOURCE_DIR}/../../" && pwd )"

if [[ ! -f "${SOURCE_DIR}/.env" ]]; then
    echo "You must create ${SOURCE_DIR}/.env"
    echo "You can use ${SOURCE_DIR}/.env.example as a template"
fi

source "${SOURCE_DIR}/.env"
source "${SOURCE_DIR}/versions.env"

header() {
    echo "============================================================"
    echo "${1}"
    echo "============================================================"
}

changelog() {
    # Strip trailing blank line
    local -r changelog=$(printf '%s\n' "$(cz ch --dry-run --unreleased-version "ADBC Libraries ${RELEASE}" --start-rev apache-arrow-adbc-${PREVIOUS_RELEASE})")
    # Split off header
    local -r header=$(echo "${changelog}" | head -n 1)
    local -r trailer=$(echo "${changelog}" | tail -n+2)
    echo "${header}"
    echo
    echo "### Versions"
    echo
    echo "- C/C++/GLib/Go/Python/Ruby: ${VERSION_NATIVE}"
    echo "- C#: ${VERSION_CSHARP}"
    echo "- Java: ${VERSION_JAVA}"
    echo "- R: ${VERSION_R}"
    echo "- Rust: ${VERSION_RUST}"
    echo "${trailer}"
}

header "Config"

echo "Repository: ${REPOSITORY}"
echo "Source Directory: ${SOURCE_TOP_DIR}"
