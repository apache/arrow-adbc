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

if [ "$#" -ne 5 ]; then
  echo "Usage: $0 <arrow-dir> <prev_veresion> <version> <next_version> <rc-num>"
  echo "Usage: $0 ../arrow 0.1.0 0.2.0 0.3.0 0"
  exit 1
fi

. $SOURCE_DIR/utils-prepare.sh

arrow_dir=$1
prev_version=$2
version=$3
next_version=$4
next_version_snapshot="${next_version}-SNAPSHOT"
rc_number=$5

export ARROW_SOURCE="$(cd "${arrow_dir}" && pwd)"

release_candidate_tag="apache-arrow-adbc-${version}-rc${rc_number}"

if [[ $(git tag -l "${release_candidate_tag}") ]]; then
    next_rc_number=$(($rc_number+1))
    echo "Tag ${release_candidate_tag} already exists, so create a new release candidate:"
    echo "1. Create or checkout maint-<version>."
    echo "2. Execute the script again with bumped RC number."
    echo "Commands:"
    echo "   git checkout maint-${version}"
    echo "   dev/release/01-prepare.sh ${version} ${next_version} ${next_rc_number}"
    exit 1
fi

############################## Pre-Tag Commits ##############################

echo "Updating changelog for $version"
# Update changelog
# XXX: commitizen doesn't respect --tag-format with --incremental, so mimic
# it by hand.
(
    echo ;
    # Strip trailing blank line
    printf '%s\n' "$(cz ch --dry-run --unreleased-version "ADBC Libraries ${version}" --start-rev apache-arrow-adbc-${prev_version})"
) >> ${SOURCE_DIR}/../../CHANGELOG.md
git add ${SOURCE_DIR}/../../CHANGELOG.md
git commit -m "chore: update CHANGELOG.md for $version"

echo "Prepare release ${version} on tag ${release_candidate_tag}"

update_versions "${version}" "${next_version}" "release"
# --allow-empty required for RCs after the first
git commit -m "chore: update versions for ${version}" --allow-empty

######################### Tag the Release Candidate #########################

git tag -a "${release_candidate_tag}" -m "ADBC Libraries ${version} RC ${rc_number}"

echo "Created release candidate tag: ${release_candidate_tag}"
echo "Push this tag before continuing!"
