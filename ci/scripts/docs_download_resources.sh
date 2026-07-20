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

# Download binary resources needed to build the docs which we can't include in
# source distributions

set -euxo pipefail

main() {
    local -r source_dir="${1}"

    local -r tmpdir="$(mktemp -d)"
    trap "rm -rf $tmpdir" EXIT

    # FontAwesome WOFF files
    # SIL Open Font License is ASF Category B
    # (the CSS is MIT and is vendored already)

    if [[ ! -f "$source_dir/docs/source/_static/fa/webfonts/fa-regular-400.woff2" ]]; then
        wget -O "$tmpdir/fontawesome.zip" https://use.fontawesome.com/releases/v7.3.1/fontawesome-free-7.3.1-web.zip
        unzip "$tmpdir/fontawesome.zip" -d "$tmpdir"
        mv "$tmpdir/fontawesome-free-7.3.1-web/webfonts/" "$source_dir/docs/source/_static/fa/webfonts/"
    else
        echo "FontAwesome webfonts already exist, skipping download"
    fi
}

main "$@"
