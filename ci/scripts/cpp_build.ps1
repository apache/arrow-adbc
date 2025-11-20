#!/usr/bin/env pwsh
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

$ErrorActionPreference = "Stop"

$SourceDir = $Args[0]
$BuildDir = $Args[1]
$InstallDir = if ($Args[2] -ne $null) { $Args[2] } else { Join-Path $BuildDir "local/" }

$BuildAll = $env:BUILD_ALL -ne "0"
$BuildDriverManager = ($BuildAll -and (-not ($env:BUILD_DRIVER_MANAGER -eq "0"))) -or ($env:BUILD_DRIVER_MANAGER -eq "1")
$BuildDriverBigQuery = ($BuildAll -and (-not ($env:BUILD_DRIVER_BIGQUERY -eq "0"))) -or ($env:BUILD_DRIVER_BIGQUERY -eq "1")
$BuildDriverFlightSql = ($BuildAll -and (-not ($env:BUILD_DRIVER_FLIGHTSQL -eq "0"))) -or ($env:BUILD_DRIVER_FLIGHTSQL -eq "1")
$BuildDriverPostgreSQL = ($BuildAll -and (-not ($env:BUILD_DRIVER_POSTGRESQL -eq "0"))) -or ($env:BUILD_DRIVER_POSTGRESQL -eq "1")
$BuildDriverSnowflake = ($BuildAll -and (-not ($env:BUILD_DRIVER_SNOWFLAKE -eq "0"))) -or ($env:BUILD_DRIVER_SNOWFLAKE -eq "1")
$BuildDriverSqlite = ($BuildAll -and (-not ($env:BUILD_DRIVER_SQLITE -eq "0"))) -or ($env:BUILD_DRIVER_SQLITE -eq "1")

$BuildDriverManagerUserConfigTest = ($env:BUILD_DRIVER_MANAGER_USER_CONFIG_TEST -eq "1")
$BuildType = if ($env:CMAKE_BUILD_TYPE) { $env:CMAKE_BUILD_TYPE } else { "Release" }

function Build-Subproject {
    New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null
    Push-Location $BuildDir

    # Note: Args are factored out to make it simple to override
    # DCMAKE_TOOLCHAIN_FILE for VCPKG
    # XXX(apache/arrow-adbc#616): must use Release build to line up with gtest
    $cmakeArgs = @(
      $(Join-Path $SourceDir "c\"),
      "-DADBC_BUILD_SHARED=ON",
      "-DADBC_BUILD_STATIC=OFF",
      "-DADBC_BUILD_TESTS=ON",
      "-DADBC_DRIVER_MANAGER=$($BuildDriverManager)",
      "-DADBC_DRIVER_BIGQUERY=$($BuildDriverBigQuery)",
      "-DADBC_DRIVER_FLIGHTSQL=$($BuildDriverFlightSql)",
      "-DADBC_DRIVER_POSTGRESQL=$($BuildDriverPostgreSQL)",
      "-DADBC_DRIVER_SNOWFLAKE=$($BuildDriverSnowflake)",
      "-DADBC_DRIVER_SQLITE=$($BuildDriverSqlite)",
      "-DCMAKE_BUILD_TYPE=$BuildType",
      "-DCMAKE_INSTALL_PREFIX=$($InstallDir)",
      "-DCMAKE_VERBOSE_MAKEFILE=ON",
      "-DADBC_DRIVER_MANAGER_TEST_MANIFEST_USER_LEVEL=$($BuildDriverManagerUserConfigTest)"
    )

    # If VCPKG_ROOT is set, set DCMAKE_TOOLCHAIN_FILE to it
    if ($env:VCPKG_ROOT) {
      $vcpkgToolchain = "$env:VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"
      $cmakeArgs += "-DCMAKE_TOOLCHAIN_FILE=$vcpkgToolchain"
    }

    & cmake $cmakeArgs
    if (-not $?) { exit 1 }

    # Build with configuration for multi-config generators (like MSVC)
    # For single-config generators (like Ninja), --config is ignored
    cmake --build . --target install --config $BuildType -j
    if (-not $?) { exit 1 }

    Pop-Location
}

Build-Subproject
