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
#
set -ue

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
    if [ "$#" -ne 1 ]; then
        echo "Usage: $0 <arrow-dir>"
        echo "Usage: $0 ../arrow"
        exit 1
    fi

    local -r arrow_dir="$1"

    echo "Release: ${RELEASE}"
    echo "Previous Release: ${PREVIOUS_RELEASE}"

    local -r tag="apache-arrow-adbc-${RELEASE}"
    if git rev-parse -q --verify "refs/tags/${tag}" >/dev/null; then
        echo "The tag ${tag} already exists."
        echo "Please update ${SOURCE_DIR}/versions.env."
        exit 1
    fi

    read -p "Please confirm that ${SOURCE_DIR}/versions.env has been updated. "

    export ARROW_SOURCE="$(cd "${arrow_dir}" && pwd)"

    ########################## Update Snapshot Version ##########################

    echo "Updating versions for ${RELEASE}-SNAPSHOT"
    update_versions "snapshot"
    git commit -m "chore: update versions for ${RELEASE}-SNAPSHOT"
    echo "Bumped versions on branch."

    ############################# Update Changelog ##############################

    git checkout apache-arrow-adbc-${PREVIOUS_RELEASE} -- CHANGELOG.md
    git commit -m "chore: update changelog for ${PREVIOUS_RELEASE}"
    echo "Updated changelog on branch."

    echo "Review the commits just made."
    echo "Then, create a new pull request."
}

main "$@"
