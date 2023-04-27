#!/bin/bash
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
set -x

# Bootstrap the Dremio docker container.

main() {
    local -r login=$(
        curl \
            -X POST \
            -H "Content-Type: application/json" \
            "$dremio_url/apiv2/login" \
            -d '{"userName":"dremio","password":"dremio123"}')
    local -r token="_dremio$(echo ${login} | jq -r .token)"

    curl \
        -X PUT \
        -H "content-type: application/json" \
        -H "authorization: ${token}" \
        "$dremio_url/apiv2/source/Samples/" \
        -d '{"config":{"externalBucketList":["samples.dremio.com"], "credentialType":"NONE","secure":false,"propertyList":[]},"name":"Samples","accelerationRefreshPeriod":3600000, "accelerationGracePeriod":10800000,"accelerationNeverRefresh":true,"accelerationNeverExpire":true,"type":"S3"}'
}

main "$@"
