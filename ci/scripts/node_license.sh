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

# Helper to assemble LICENSE.txt for the JavaScript native addon,
# covering the Rust crates statically linked into the binary.
# https://infra.apache.org/licensing-howto.html

set -e
set -o pipefail
set -u

main() {
    local -r source_dir="${1}"
    local -r output="${2}"

    local -r tmp_file="${output}.tmp"

    # Generate third-party Rust crate license attributions.
    # Must run before writing to ${output} because cargo-about scans the
    # package directory for license files; if LICENSE.txt already exists
    # there it creates a separate license entry that breaks grouping.
    pushd "${source_dir}/javascript"
    cargo about generate license.hbs > "${tmp_file}"
    popd

    # Prepend the Apache 2.0 header that covers our own code.
    head -202 "${source_dir}/LICENSE.txt" > "${output}"
    cat "${tmp_file}" >> "${output}"
    rm "${tmp_file}"
}

main "$@"
