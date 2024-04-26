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

set -eu

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
    if [ "$#" -ne 1 ]; then
        echo "Usage: $0 <rc-num>"
        exit 1
    fi

    local -r rc_number="$1"
    local -r tag="apache-arrow-adbc-${RELEASE}-rc${rc_number}"
    local -r tarball="apache-arrow-adbc-${RELEASE}.tar.gz"

    header "Downloading assets from release"
    local -r download_dir="packages/${tag}"
    mkdir -p "${download_dir}"
    gh release download \
       "${tag}" \
       --dir "${download_dir}" \
       --pattern "${tarball}*" \
       --repo "${REPOSITORY}" \
       --skip-existing

    echo "Uploading to dist.apache.org"

    # check out the arrow RC folder
    svn co --depth=empty https://dist.apache.org/repos/dist/dev/arrow tmp

    # add the release candidate for the tag
    mkdir -p "tmp/${tag}"

    # copy the rc tarball into the tmp dir
    cp ${download_dir}/${tarball}* "tmp/${tag}"

    # commit to svn
    svn add "tmp/${tag}"
    if [[ ${DRY_RUN} -eq 0 ]]; then
        svn ci -m "Apache Arrow ADBC ${RELEASE} RC${rc_number}" "tmp/${tag}"
    else
        echo "Dry run: not committing to dist.apache.org"
    fi

    # clean up
    rm -rf tmp

    echo "Uploaded at https://dist.apache.org/repos/dist/dev/arrow/${tag}"
}

main "$@"
