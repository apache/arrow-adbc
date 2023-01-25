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

PYTHON_VERSIONS="3.9 3.10 3.11"

main() {
    if [[ "$#" != 2 ]]; then
       echo "Usage: ${0} <adbc-checkout> <output-path>"
       exit 1
    fi
    # Path to ADBC repo
    local -r source_dir="${1}"
    # Path to output directory
    local -r output="${2}"

    # For $COMPONENTS, test_packages
    source "${source_dir}/ci/scripts/python_util.sh"

    eval "$(conda shell.bash hook)"

    for python_version in $PYTHON_VERSIONS; do
        echo "=== Testing Python ${python_version} ==="
        local env="${output}/conda-test/${python_version}"
        mkdir -p "${env}"

        mamba create \
              -y \
              -p "${env}" \
              -c conda-forge \
              --file "${source_dir}/ci/conda_env_python.txt" \
              python="${python_version}" \
              pyarrow

        conda activate "${env}"

        mamba install \
              -y \
              -c "${output}/conda" \
              ${COMPONENTS//_/-}

        test_packages
    done
}

main "$@"
