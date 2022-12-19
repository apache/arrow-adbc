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

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <version> <next_version>"
  exit 1
fi

. $SOURCE_DIR/utils-prepare.sh

version=$1
next_version=$2
next_version_snapshot="${next_version}-SNAPSHOT"

########################## Update Snapshot Version ##########################

git fetch --all --prune --tags --force -j$(nproc)
git switch main
git rebase apache/main

echo "Updating versions for ${next_version_snapshot}"
update_versions "${version}" "${next_version}" "snapshot"
git commit -m "chore: update versions for ${next_version_snapshot}"
echo "Bumped versions on branch."

echo "Pushing changes to apache/arrow-adbc:main"
git push apache main
