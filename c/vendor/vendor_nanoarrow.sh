#!/bin/bash
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

# Download and extract the latest Nanoarrow.

main() {
    local -r repo_url="https://github.com/apache/arrow-nanoarrow"
    # Check releases page: https://github.com/apache/arrow-nanoarrow/releases/
    local -r commit_sha=33d2c8b973d8f8f424e02ac92ddeaace2a92f8dd

    echo "Fetching $commit_sha from $repo_url"
    SCRATCH=$(mktemp -d)
    trap 'rm -rf "$SCRATCH"' EXIT
    local -r tarball="$SCRATCH/nanoarrow.tar.gz"
    wget -O "$tarball" "$repo_url/archive/$commit_sha.tar.gz"

    mv nanoarrow/CMakeLists.txt CMakeLists.nanoarrow.tmp
    rm -rf nanoarrow
    mkdir -p nanoarrow
    tar --strip-components 1 -C "$SCRATCH" -xf "$tarball"

    # Build the bundle
    python "$SCRATCH/ci/scripts/bundle.py" \
        --symbol-namespace=Private \
        --include-output-dir=nanoarrow \
        --source-output-dir=nanoarrow \
        --header-namespace=

    mv CMakeLists.nanoarrow.tmp nanoarrow/CMakeLists.txt
}

main "$@"
