#!/usr/bin/env bash
# -*- indent-tabs-mode: nil; sh-indentation: 2; sh-basic-offset: 2 -*-
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

set -e
set -u
set -o pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
  if [ "$#" -ne 0 ]; then
    echo "Usage: $0"
    exit
  fi

  local -r tag="apache-arrow-adbc-${RELEASE}"
  local -r tmp=$(mktemp -d -t "arrow-post-npm.XXXXX")

  header "Downloading Node.js packages for ${RELEASE}"

  gh release download \
     --repo "${REPOSITORY}" \
     "${tag}" \
     --dir "${tmp}" \
     --pattern "apache-arrow-adbc-driver-manager-*.tgz"

  header "Uploading Node.js packages for ${RELEASE}"

  DRY_RUN="${DRY_RUN:-0}" "${SOURCE_TOP_DIR}/ci/scripts/node_npm_upload.sh" "${tmp}"

  rm -rf "${tmp}"

  echo "Success! The released npm package is available here:"
  echo "  https://www.npmjs.com/package/@apache-arrow/adbc-driver-manager"
}

main "$@"
