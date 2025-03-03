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

# arrow-c-data-18.2.0-sources.jar ->
# jar
extract_type() {
  local path="$1"
  echo "${path}" | grep -o "[^.]*$"
}

# arrow-c-data-18.2.0-sources.jar arrow-c-data-18.2.0 ->
# sources
extract_classifier() {
  local path="$1"
  local base="$2"
  basename "${path}" | sed -e "s/^${base}-//g" -e "s/\.[^.]*$//g"
}

main() {
    if [ $# -ne 1 ]; then
        echo "Usage: $0 <rc-number>"
        echo "Usage: $0 0"
        exit
    fi

    local -r rc_number="$1"
    local -r tag="apache-arrow-adbc-${RELEASE}-rc${rc_number}"

    export ARROW_ARTIFACTS_DIR="$(pwd)/packages/${tag}/java"
    rm -rf "${ARROW_ARTIFACTS_DIR}"
    mkdir -p "${ARROW_ARTIFACTS_DIR}"
    gh release download \
       --dir "${ARROW_ARTIFACTS_DIR}" \
       --pattern "*.jar" \
       --pattern "*.jar.asc" \
       --pattern "*.pom" \
       --pattern "*.pom.asc" \
       --repo "${REPOSITORY}" \
       "${tag}"

    pushd "${ARROW_ARTIFACTS_DIR}"

    for pom in *.pom; do
        base=$(basename "${pom}" .pom)
        files=()
        types=()
        classifiers=()
        args=()  # Args to Maven
        args+=(deploy:deploy-file)
        args+=(-Durl=https://repository.apache.org/service/local/staging/deploy/maven2)
        args+=(-DrepositoryId=apache.releases.https)
        args+=(-DretryFailedDeploymentCount=10)
        args+=(-DpomFile="${pom}")
        if [ -f "${base}.jar" ]; then
            jar="${base}.jar"
            args+=(-Dfile="${jar}")
            files+=("${base}.jar.asc")
            types+=("jar.asc")
            classifiers+=("")
        else
            args+=(-Dfile="${pom}")
        fi
        files+=("${base}.pom.asc")
        types+=("pom.asc")
        classifiers+=("")
        if [ "$(echo "${base}"-*)" != "${base}-*" ]; then
            for other_file in "${base}"-*; do
                type="$(extract_type "${other_file}")"
                case "${type}" in
                    asc | sha256 | sha512)
                        continue
                        ;;
                esac
                classifier=$(extract_classifier "${other_file}" "${base}")
                files+=("${other_file}")
                types+=("${type}")
                classifiers+=("${classifier}")
                other_file_base="$(basename "${other_file}")"
                files+=("${other_file_base}.asc")
                types+=("${type}.asc")
                classifiers+=("${classifier}")
            done
        fi
        args+=(-Dfiles="$(
      IFS=,
      echo "${files[*]}"
    )")
        args+=(-Dtypes="$(
      IFS=,
      echo "${types[*]}"
    )")
        args+=(-Dclassifiers="$(
      IFS=,
      echo "${classifiers[*]}"
    )")
        mvn "${args[@]}"
    done

    popd

    echo "Success!"
    echo "Press the 'Close' button in the web interface:"
    echo "    https://repository.apache.org/#stagingRepositories"
    echo "It publishes the artifacts to the staging repository:"
    echo "    https://repository.apache.org/content/repositories/staging/org/apache/arrow/"
    read -p "After publishing the artifacts, press ENTER to continue..." ignored
}

main "$@"
