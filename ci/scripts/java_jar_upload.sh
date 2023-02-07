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

# Usage: java_jar_upload.sh jar1.pom jar2.pom ...

retry() {
    local -r retries="${1}"
    shift

    local attempt=0
    while ! "$@"; do
        local last_status="$?"
        attempt=$((attempt + 1))
        if [[ "${attempt}" -lt "${retries}" ]]; then
            local delay=$((2 ** ${attempt}))
            echo "Attempt ${attempt}/${retries}, waiting ${delay} seconds"
            sleep "${delay}"
        else
            echo "Attempt ${attempt}/${retries}, exiting"
            exit "${last_status}"
        fi
    done
    return 0
}

main() {
    if [[ -z "${GEMFURY_PUSH_TOKEN}" ]]; then
        echo "GEMFURY_PUSH_TOKEN must be set"
        exit 1
    fi

    local settings_file=$(mktemp adbc.settingsXXXXXXXX)
    trap 'rm -f "$settings_file"' ERR EXIT INT TERM
    cat <<SETTINGS > "${settings_file}"
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
          xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
          xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 https://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>fury</id>
      <username>${GEMFURY_PUSH_TOKEN}</username>
      <password>NOPASS</password>
      <configuration>
        <httpConfiguration>
          <all>
            <usePreemptive>true</usePreemptive>
          </all>
        </httpConfiguration>
      </configuration>
    </server>
  </servers>
</settings>
SETTINGS

    local -r is_root='^arrow-adbc-java-root-.*'
    for pom in "$@"; do
        echo "Deploying ${pom}"
        local mvnArgs=""

        local filename=$(basename "${pom}" .pom)
        local jar=$(dirname "${pom}")/"${filename}.jar"
        local sources=$(dirname "${pom}")/"${filename}-javadoc.jar"
        local javadoc=$(dirname "${pom}")/"${filename}-sources.jar"

        if [[ -f "${sources}" ]]; then
            mvnArgs="${mvnArgs} -Dsources=${sources}"
        fi

        if [[ -f "${javadoc}" ]]; then
            mvnArgs="${mvnArgs} -Djavadoc=${javadoc}"
        fi

        # apache/arrow-adbc#285: Gemfury appears to be flaky with some
        # 503s, so retry each upload.
        if [[ "${filename}" =~ $is_root ]]; then
            # The root is POM-only.
            retry 3 mvn \
                  -Dmaven.install.skip=true \
                  -Drat.skip=true \
                  -DskipTests \
                  --settings "${settings_file}" \
                  deploy:deploy-file \
                  -DrepositoryId=fury \
                  -Durl=https://maven.fury.io/arrow-adbc-nightlies/ \
                  -DgeneratePom=false \
                  -Dfile="${pom}" \
                  -DpomFile="${pom}" \
                  ${mvnArgs}
        else
            retry 3 mvn \
                  -Dmaven.install.skip=true \
                  -Drat.skip=true \
                  -DskipTests \
                  --settings "${settings_file}" \
                  deploy:deploy-file \
                  -DrepositoryId=fury \
                  -Durl=https://maven.fury.io/arrow-adbc-nightlies/ \
                  -DgeneratePom=false \
                  -Dfile="${jar}" \
                  -DpomFile="${pom}" \
                  ${mvnArgs}
        fi
    done
}

main "$@"
