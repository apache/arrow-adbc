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

set -ex

main() {
    local -r source_dir=${1}
    local -r dist_dir=${2}

    echo "=== Clean artifacts from local Maven repository ==="
    # sed is for removing "^[[0m" at the end
    local -r maven_repo=$(mvn help:evaluate -Dexpression=settings.localRepository -q -DforceStdout | sed -e 's/\x1b\[0m//')/org/apache/arrow/adbc
    if [[ -d "${maven_repo}" ]]; then
        find "${maven_repo}" \
             "(" -name "*.jar" -o -name "*.zip" -o -name "*.pom" ")" \
             -exec echo {} ";" \
             -delete
    fi

    echo "=== Build ==="
    pushd ${source_dir}/java
    mvn -B clean \
        install \
        -Papache-release \
        -T 2C \
        -DskipTests \
        -Dgpg.skip
    popd

    if [[ -z "${dist_dir}" ]]; then
        echo "=== No dist_dir provided, skipping artifact copy ==="
    else
        echo "=== Copying artifacts to dist dir ==="
        mkdir -p "${dist_dir}"

        find "${maven_repo}" \
             "(" -name "*.jar" -o -name "*.pom" ")" \
             -exec echo {} ";" \
             -exec cp {} "${dist_dir}" ";"
    fi
}

main "$@"
