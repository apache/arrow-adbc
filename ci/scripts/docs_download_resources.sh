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

    # FontAwesome
    # WOFF files are under SIL Open Font License (ASF Category B)
    # CSS file is under MIT
    if [[ ! -f "$source_dir/docs/source/_static/fontawesome/css/all.min.css" ]]; then
        wget -O "$tmpdir/fontawesome.tgz" https://github.com/FortAwesome/Font-Awesome/archive/refs/tags/7.3.1.tar.gz
        pushd $tmpdir
        tar xvf "$tmpdir/fontawesome.tgz"
        popd

        mkdir -p "$source_dir/docs/source/_static/fontawesome/css/"
        mv "$tmpdir/Font-Awesome-7.3.1/webfonts/" "$source_dir/docs/source/_static/fontawesome/webfonts/"
        cp "$tmpdir/Font-Awesome-7.3.1/css/all.min.css" "$source_dir/docs/source/_static/fontawesome/css/"
    else
        echo "FontAwesome webfonts already exist, skipping download"
    fi
}

main "$@"
