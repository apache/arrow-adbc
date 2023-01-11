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

main() {
    if [ "$#" -ne 1 ]; then
        echo "Usage: $0 <version>"
        echo "Usage: $0 1.0.0"
        exit 1
    fi

    local -r version="$1"

    archive_name=apache-arrow-adbc-${version}

    tar_gz=${archive_name}.tar.gz

    rm -f ${tar_gz}
    curl \
      --remote-name \
      --fail \
      https://downloads.apache.org/arrow/apache-arrow-adbc-${version}/${tar_gz}
    rm -rf ${archive_name}
    tar xf ${tar_gz}

    read -p "Please enter your RubyGems MFA one-time password (or leave empty if you don't have MFA enabled): " GEM_HOST_OTP_CODE </dev/tty
    export GEM_HOST_OTP_CODE

    pushd ${archive_name}/ruby
    rake release
    popd

    rm -rf ${archive_name}
    rm -f ${tar_gz}

    echo "Success! The released RubyGems are available here:"
    echo "  https://rubygems.org/gems/red-adbc/versions/${version}"
}

main "$@"
