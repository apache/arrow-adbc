#!/usr/bin/env bash
# -*- indent-tabs-mode: nil; sh-indentation: 2; sh-basic-offset: 2 -*-
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
#

set -e
set -u
set -o pipefail

SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
source "${SOURCE_DIR}/utils-common.sh"
source "${SOURCE_DIR}/utils-prepare.sh"

main() {
  if [ "$#" -ne 0 ]; then
    echo "Usage: $0"
    exit
  fi

  local -r tag="apache-arrow-adbc-${RELEASE}"

  if [ -z "${NUGET_API_KEY}" ]; then
    echo "NUGET_API_KEY is empty"
    exit 1
  fi

  local -r tmp=$(mktemp -d -t "arrow-post-csharp.XXXXX")

  header "Downloading C# packages for ${VERSION_CSHARP}"

  gh release download \
     --repo "${REPOSITORY}" \
     "${tag}" \
     --dir "${tmp}" \
     --pattern "*.nupkg" \
     --pattern "*.snupkg"

  local base_names=()
  base_names+=(Apache.Arrow.Adbc.${VERSION_CSHARP})
  base_names+=(Apache.Arrow.Adbc.Client.${VERSION_CSHARP})
  base_names+=(Apache.Arrow.Adbc.Drivers.Apache.${VERSION_CSHARP})
  base_names+=(Apache.Arrow.Adbc.Drivers.BigQuery.${VERSION_CSHARP})
  base_names+=(Apache.Arrow.Adbc.Drivers.Databricks.${VERSION_CSHARP})
  base_names+=(Apache.Arrow.Adbc.Drivers.Interop.FlightSql.${VERSION_CSHARP})
  base_names+=(Apache.Arrow.Adbc.Drivers.Interop.Snowflake.${VERSION_CSHARP})
  for base_name in "${base_names[@]}"; do
    dotnet nuget push \
      "${tmp}/${base_name}.nupkg" \
      -k ${NUGET_API_KEY} \
      -s https://api.nuget.org/v3/index.json
  done

  rm -rf "${tmp}"

  echo "Success! The released NuGet package is available here:"
  echo "  https://www.nuget.org/packages/Apache.Arrow.Adbc/${VERSION_CSHARP}"
}

main "$@"
