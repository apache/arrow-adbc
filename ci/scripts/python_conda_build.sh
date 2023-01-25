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

# Build the Conda packages

set -e
set -o pipefail
set -u
set -x

main() {
    if [[ "$#" != 3 ]]; then
       echo "Usage: ${0} <adbc-checkout> <platform-config> <output-path>"
       echo "<platform-config> should be the name of a YAML file in ci/conda/.ci_support."
       exit 1
    fi
    # Path to ADBC repo
    local -r repo="${1}"
    local -r platform_config="${repo}/ci/conda/.ci_support/${2}"
    # Path to output directory
    local -r output="${3}"

    mkdir -p "${output}"
    export PYTHONUNBUFFERED=1

    conda config --show

    conda mambabuild \
          "${repo}/ci/conda" \
          -m "${platform_config}" \
          --output-folder "${output}/conda"
}

main "$@"
