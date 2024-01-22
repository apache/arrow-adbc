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
  echo "Usage: $0 <arrow-dir> <version> <next_version>"
  echo "Usage: $0 ../arrow 0.1.0 1.0.0"
  exit 1
fi

. $SOURCE_DIR/utils-prepare.sh

arrow_dir=$1
version=$2
next_version=$3
next_version_snapshot="${next_version}-SNAPSHOT"

export ARROW_SOURCE="$(cd "${arrow_dir}" && pwd)"

########################## Update Snapshot Version ##########################

git fetch --all --prune --tags --force -j$(nproc)
git switch main
git rebase apache/main

echo "Updating versions for ${next_version_snapshot}"
update_versions "${version}" "${next_version}" "snapshot"
git commit -m "chore: update versions for ${next_version_snapshot}"
echo "Bumped versions on branch."

############################# Update Changelog ##############################

git checkout apache-arrow-adbc-${version} -- CHANGELOG.md
git commit -m "chore: update changelog for ${version}"
echo "Updated changelog on branch."

echo "Review the commits just made."
echo "Then, push changes to apache/arrow-adbc:main with:"
echo git push apache main
