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

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <version> <next_version> <rc-num>"
  exit 1
fi

. $SOURCE_DIR/utils-prepare.sh

version=$1
next_version=$2
next_version_snapshot="${next_version}-SNAPSHOT"
rc_number=$3

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
cz ch --incremental --unreleased-version "ADBC Libraries ${version} RC ${rc_number}"
git add ${SOURCE_DIR}/../../CHANGELOG.md
git commit -m "chore: update CHANGELOG.md for $version"

echo "Prepare release ${version} on tag ${release_candidate_tag}"

update_versions "${version}" "${next_version}" "release"
git commit -m "chore: update versions for ${version}"

######################### Tag the Release Candidate #########################

git tag -a "${release_candidate_tag}" -m "ADBC Libraries ${version} RC ${rc_number}"

echo "Created release candidate tag: ${release_candidate_tag}"
echo "Push this tag before continuing!"
