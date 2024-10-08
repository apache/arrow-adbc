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

# Ensure the headers in go/adbc/drivermgr/ match the actual headers
# since Go doesn't package symlinks we need to have actual copies
# of the files. So we need to make sure they stay in sync.

set -e

main() {
  local -r source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  local -r source_top_dir="$(cd "${source_dir}/../../" && pwd)"

  pushd "${source_top_dir}"

  for f in "$@"; do
    fn=$(basename $f)
    if ! diff -q "$f" "go/adbc/drivermgr/arrow-adbc/$fn" &>/dev/null; then
      >&2 echo "OUT OF SYNC: $f differs from go/adbc/drivermgr/arrow-adbc/$fn"
      popd
      return 1
    fi
  done

  popd
}

main "$@"
