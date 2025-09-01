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
  # Ensure we are being run from the tag
  if [[ $(git describe --exact-match --tags) != "${tag}" ]]; then
    echo "This script must be run from the tag ${tag}"
    exit 1
  fi

  pushd "${SOURCE_TOP_DIR}/rust"
  cargo publish --all-features -p adbc_core
  cargo publish --all-features -p adbc_ffi
  cargo publish --all-features -p adbc_driver_manager
  cargo publish --all-features -p adbc_datafusion
  cargo publish --all-features -p adbc_snowflake
  popd

  echo "Success! The released Cargo crates are available here:"
  echo "  https://crates.io/crates/adbc_core"
  echo "  https://crates.io/crates/adbc_driver_ffi"
  echo "  https://crates.io/crates/adbc_driver_manager"
  echo "  https://crates.io/crates/adbc_datafusion"
  echo "  https://crates.io/crates/adbc_snowflake"
}

main "$@"
