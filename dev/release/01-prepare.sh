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
source "$SOURCE_DIR/utils-common.sh"
source "$SOURCE_DIR/utils-prepare.sh"

main() {
    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <arrow-dir> <rc_num>"
        echo "Usage: $0 ../arrow 0"
        exit 1
    fi

    local -r arrow_dir="$1"
    local -r rc_number="$2"
    local -r release_candidate_tag="apache-arrow-adbc-${RELEASE}-rc${rc_number}"

    export ARROW_SOURCE="$(cd "${arrow_dir}" && pwd)"

    if [[ $(git tag -l "${release_candidate_tag}") ]]; then
        local -r next_rc_number=$(($rc_number+1))
        echo "Tag ${release_candidate_tag} already exists, so create a new release candidate:"
        echo "1. Create or checkout maint-<version>."
        echo "2. Execute the script again with bumped RC number."
        echo "Commands:"
        echo "   git checkout maint-${RELEASE}"
        echo "   dev/release/01-prepare.sh ${arrow_dir} ${next_rc_number}"
        exit 1
    fi

    ############################## Pre-Tag Commits ##############################

    header "Updating changelog for ${RELEASE}"
    # Update changelog
    # XXX: commitizen doesn't respect --tag-format with --incremental, so mimic
    # it by hand.
    (
        echo ;
        changelog
    ) >> ${SOURCE_DIR}/../../CHANGELOG.md

    read -p "Please review the changelog. Press ENTER to continue..." ignored
    git diff ${SOURCE_DIR}/../../CHANGELOG.md

    echo "Is the changelog correct?"
    select yn in "y" "n"; do
        case $yn in
            y ) echo "Continuing"; break;;
            n ) echo "Aborting"; return 1;;
        esac
    done

    git add ${SOURCE_DIR}/../../CHANGELOG.md
    git commit -m "chore: update CHANGELOG.md for ${RELEASE}"

    header "Prepare release ${RELEASE} on tag ${release_candidate_tag}"

    update_versions "release"
    # --allow-empty required for RCs after the first
    git commit -m "chore: update versions for ${RELEASE}" --allow-empty

    ######################### Tag the Release Candidate #########################

    header "Tag the release candidate ${release_candidate_tag}"

    git tag -a "${release_candidate_tag}" -m "ADBC Libraries ${RELEASE} RC ${rc_number}"

    echo "Created release candidate tag: ${release_candidate_tag}"
    echo "Push this tag before continuing!"
}

main "$@"
