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

set -euo pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
    if [ $# -ne 1 ]; then
        echo "Usage: $0 <rc-number>"
        echo "Usage: $0 0"
        exit
    fi

    local -r rc_number="$1"
    local -r tag="apache-arrow-adbc-${RELEASE}-rc${rc_number}"

    echo "Starting GitHub Actions workflow on ${REPOSITORY} for ${RELEASE} RC${rc_number}"

    gh workflow run \
       --repo "${REPOSITORY}" \
       --ref "${WORKFLOW_REF}" \
       verify.yml \
       --raw-field version="${RELEASE}" \
       --raw-field rc="${rc_number}"

    local run_id=""
    while [[ -z "${run_id}" ]]
    do
        echo "Waiting for run to start..."
        run_id=$(gh run list \
                    --repo "${REPOSITORY}" \
                    --workflow=verify.yml \
                    --json 'databaseId,event,headBranch,status' \
                    --jq ".[] | select(.event == \"workflow_dispatch\" and .headBranch == \"${WORKFLOW_REF}\" and .status != \"completed\") | .databaseId")
        sleep 1
    done

    echo "Started GitHub Actions workflow with ID: ${run_id}"
    echo "You can wait for completion via: gh run watch --repo ${REPOSITORY} ${run_id}"

    set_resolved_issues "${RELEASE}"

    # Embed source tarball hash into the email. Assume directory structure
    # from 02-sign.sh.
    local -r download_dir="packages/${tag}"
    local -r SOURCE_TARBALL_HASH=$(cat $(find "${download_dir}" -type f -name "apache-arrow-adbc-${RELEASE}*.tar.gz.sha512") | awk '{print $1}')

    echo "The following draft email has been created to send to the"
    echo "dev@arrow.apache.org mailing list"
    echo ""
    echo "---------------------------------------------------------"

    local -r commit=$(git rev-list -n 1 "${tag}")

    cat <<MAIL
To: dev@arrow.apache.org
Subject: [VOTE] Release Apache Arrow ADBC ${RELEASE} - RC${rc_number}

Hello,

I would like to propose the following release candidate (RC${rc_number}) of Apache Arrow ADBC version ${RELEASE}. This is a release consisting of ${RESOLVED_ISSUES} resolved GitHub issues [1].

The subcomponents are versioned independently:

- C/C++/GLib/Go/Python/Ruby: ${VERSION_NATIVE}
- C#: ${VERSION_CSHARP}
- Java: ${VERSION_JAVA}
- R: ${VERSION_R}
- Rust: ${VERSION_RUST}

This release candidate is based on commit: ${commit} [2]

The source release rc${rc_number} is hosted at [3].
This is not a permanent URL. If the RC is accepted, it will be moved to the final release location.
The SHA512 hash of the source tarball is:
${SOURCE_TARBALL_HASH}

The binary artifacts are hosted at [4][5][6][7][8].
The changelog is located at [9].

Please download, verify checksums and signatures, run the unit tests, and vote on the release. See [10] for how to validate a release candidate.

See also a verification result on GitHub Actions [11].

The vote will be open for at least 72 hours.

[ ] +1 Release this as Apache Arrow ADBC ${RELEASE}
[ ] +0
[ ] -1 Do not release this as Apache Arrow ADBC ${RELEASE} because...

Note: to verify APT/YUM packages on macOS/AArch64, you must \`export DOCKER_DEFAULT_PLATFORM=linux/amd64\`. (Or skip this step by \`export TEST_APT=0 TEST_YUM=0\`.)

[1]: https://github.com/apache/arrow-adbc/issues?q=is%3Aissue+milestone%3A%22ADBC+Libraries+${RELEASE}%22+is%3Aclosed
[2]: https://github.com/apache/arrow-adbc/commit/${commit}
[3]: https://dist.apache.org/repos/dist/dev/arrow/${tag}/
[4]: https://packages.apache.org/artifactory/arrow/almalinux-rc/
[5]: https://packages.apache.org/artifactory/arrow/debian-rc/
[6]: https://packages.apache.org/artifactory/arrow/ubuntu-rc/
[7]: https://repository.apache.org/content/repositories/staging/org/apache/arrow/adbc/
[8]: https://github.com/apache/arrow-adbc/releases/tag/${tag}
[9]: https://github.com/apache/arrow-adbc/blob/${tag}/CHANGELOG.md
[10]: https://arrow.apache.org/adbc/main/development/releasing.html#how-to-verify-release-candidates
[11]: https://github.com/apache/arrow-adbc/actions/runs/${run_id}
MAIL
}

main "$@"
