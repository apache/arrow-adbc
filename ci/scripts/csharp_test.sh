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

set -ex

source_dir=${1}/csharp/test/Apache.Arrow.Adbc.Tests

pushd ${source_dir}

# The test project targets net472 on every platform so that build is always
# compiled, but net472 tests can only be *run* on Windows -- there is no .NET
# Framework test host on Linux or macOS. On those platforms run the remaining
# target frameworks one at a time. The list is read back from the project so
# that adding a target framework does not silently drop it from CI.
case "$(uname -s)" in
  MINGW*|MSYS*|CYGWIN*)
    dotnet test
    ;;
  *)
    target_frameworks=$(dotnet msbuild Apache.Arrow.Adbc.Testing.csproj \
                          -getProperty:TargetFrameworks -nologo | tr -d '\r')
    for target_framework in ${target_frameworks//;/ }; do
      if [ "${target_framework}" != "net472" ]; then
        dotnet test -f "${target_framework}"
      fi
    done
    ;;
esac

popd

# Databricks driver has been moved out of this repo; its tests are kept
# on disk for now but are intentionally not built or run in CI.
# source_dir=${1}/csharp/test/Drivers/Databricks
# pushd ${source_dir}
# dotnet test --filter "FullyQualifiedName~Apache.Arrow.Adbc.Tests.Drivers.Databricks.Unit"
# popd
