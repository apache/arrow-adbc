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

: ${SOURCE_UPLOAD:="0"}

main() {
    local -r source_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    local -r source_top_dir="$( cd "${source_dir}/../../" && pwd )"

    if type shasum >/dev/null 2>&1; then
        local -r sha256_generate="shasum -a 256"
        local -r sha512_generate="shasum -a 512"
    else
        local -r sha256_generate="sha256sum"
        local -r sha512_generate="sha512sum"
    fi

    if [ "$#" -ne 2 ]; then
        echo "Usage: $0 <version> <rc-num>"
        exit 1
    fi
    local -r version="$1"
    local -r rc_number="$2"
    local -r tag="apache-arrow-adbc-${version}-rc${rc_number}"

    echo "Preparing source for tag ${tag}"
    local -r release_hash=$(cd "${source_top_dir}" && git rev-list --max-count=1 ${tag} --)

    if [[ -z "${release_hash}" ]]; then
        echo "Cannot continue: unknown Git tag: ${tag}"
        exit 1
    fi

    echo "Using commit ${release_hash}"

    local -r tarball="${tag}.tar.gz"

    pushd "${source_top_dir}"

    rm -rf "${tag}/"
    git archive "${release_hash}" --prefix "${tag}/" | tar xf -

    # Resolve all hard and symbolic links
    rm -rf "${tag}.tmp/"
    mv "${tag}/" "${tag}.tmp/"
    cp -R -L "${tag}.tmp" "${tag}"
    rm -rf "${tag}.tmp/"

    # Create new tarball
    tar czf "${tarball}" "${tag}/"
    rm -rf "${tag}/"

    # check licenses
    "${source_dir}/run-rat.sh" "${tarball}"

    # Sign the archive
    gpg --armor --output "${tarball}.asc" --detach-sig "${tarball}"
    ${sha256_generate} "${tarball}" | tee "${tarball}.sha256"
    ${sha512_generate} "${tarball}" | tee "${tarball}.sha512"

    # Upload
    if [[ "${SOURCE_UPLOAD}" -gt 0 ]]; then
        echo "Uploading to dist.apache.org"

        local -r tagrc="${tag}"

        # check out the arrow RC folder
        svn co --depth=empty https://dist.apache.org/repos/dist/dev/arrow tmp

        # add the release candidate for the tag
        mkdir -p "tmp/${tagrc}"

        # copy the rc tarball into the tmp dir
        cp ${tarball}* "tmp/${tagrc}"

        # commit to svn
        svn add "tmp/${tagrc}"
        svn ci -m "Apache Arrow ADBC ${version} RC${rc}" "tmp/${tagrc}"

        # clean up
        rm -rf tmp

        echo "Uploaded at https://dist.apache.org/repos/dist/dev/arrow/${tagrc}"
    fi

    echo "Commit SHA1: ${release_hash}"

    popd
}

main "$@"
