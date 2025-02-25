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

# Re-run mamba if it flakes.

# set -euxo pipefail

main() {
    local count=0
    while [[ $count -lt 5 ]]; do
        # https://stackoverflow.com/questions/12451278
        exec 5>&1
        MAMBA_OUTPUT=$(mamba "$@" 2>&1 | tee /dev/fd/5; exit ${PIPESTATUS[0]})
        exit_code=$?
        if [[ $exit_code -eq 0 ]]; then
            echo "Mamba succeeded!"
            return 0
        fi

        count=$((count + 1))

        if echo $MAMBA_OUTPUT | grep "Found incorrect download" >/dev/null; then
            echo "Mamba flaked..."
            continue
        fi
        echo "Mamba failed, aborting"
        return $exit_code
    done
    echo "Mamba flaked too many times, aborting"
    exit 1
}

main "$@"
