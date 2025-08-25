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

: ${DRY_RUN:=0}
: ${REPOSITORY:="apache/arrow-adbc"}
: ${WORKFLOW_REF:="main"}

SOURCE_TOP_DIR="$( cd "${SOURCE_DIR}/../../" && pwd )"

if [[ ! -f "${SOURCE_DIR}/.env" ]]; then
    echo "You must create ${SOURCE_DIR}/.env"
    echo "You can use ${SOURCE_DIR}/.env.example as a template"
fi

source "${SOURCE_DIR}/.env"
source "${SOURCE_DIR}/versions.env"

header() {
    echo "============================================================"
    echo "${1}"
    echo "============================================================"
}

changelog() {
    # Strip trailing blank line
    env PYTHONPATH=${SOURCE_TOP_DIR}/dev python -m adbc_dev.changelog --name "ADBC Libraries ${RELEASE}" apache-arrow-adbc-${PREVIOUS_RELEASE} HEAD 2>/dev/null
}

header "Config"

echo "Repository: ${REPOSITORY}"
echo "Source Directory: ${SOURCE_TOP_DIR}"
