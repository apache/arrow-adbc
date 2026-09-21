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

set -e
set -u
set -o pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
    if [ "$#" -ne 0 ]; then
        echo "Usage: $0"
        exit 1
    fi

    header "Tagging Go release ${VERSION_NATIVE}"

    version_tag="apache-arrow-adbc-${RELEASE}"
    go_adbc_tag="go/adbc/v${VERSION_NATIVE}"
    go_driver_tag="go/driver/v${VERSION_NATIVE}"

    git tag "${go_adbc_tag}" "${version_tag}"
    echo "Created tag ${go_adbc_tag}"
    echo "Please verify and push the tag:"
    echo git push apache "${go_adbc_tag}"

    read -p "After pushing the tag, press ENTER to continue..." ignored

    git switch -c "go-driver-${VERSION_NATIVE}" "${version_tag}"
    pushd go/driver
    go get -u github.com/apache/arrow-adbc/go/adbc@"${VERSION_NATIVE}"
    go mod tidy
    popd

    git add go/driver/go.mod go/driver/go.sum
    git commit -m "chore: update go.mod and go.sum for ${VERSION_NATIVE}"
    read -p "Verify the commit, then press ENTER to continue..." ignored
    git tag "${go_driver_tag}" "go-driver-${VERSION_NATIVE}"
    echo "Created tag ${go_driver_tag}"
    echo "Please verify and push the tag:"
    echo git push apache "${go_driver_tag}"
    read -p "After pushing the tag, press ENTER to continue..." ignored
}

main "$@"
