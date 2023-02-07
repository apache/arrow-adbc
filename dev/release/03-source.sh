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

main() {
    local -r source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local -r source_top_dir="$( cd "${source_dir}/../../" && pwd )"

    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <version> <rc-num>"
        exit 1
    fi
    local -r version="$1"
    local -r rc_number="$2"
    local -r tag="apache-arrow-adbc-${version}-rc${rc_number}"
    local -r tarball="apache-arrow-adbc-${version}.tar.gz"

    : ${REPOSITORY:="apache/arrow-adbc"}

    if [[ ! -f "${source_dir}/.env" ]]; then
        echo "You must create ${source_dir}/.env"
        echo "You can use ${source_dir}/.env.example as a template"
    fi

    source "${source_dir}/.env"

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
    svn ci -m "Apache Arrow ADBC ${version} RC${rc_number}" "tmp/${tag}"

    # clean up
    rm -rf tmp

    echo "Uploaded at https://dist.apache.org/repos/dist/dev/arrow/${tag}"
}

header() {
    echo "============================================================"
    echo "${1}"
    echo "============================================================"
}

main "$@"
