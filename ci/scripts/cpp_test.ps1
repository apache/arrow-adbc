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

$BuildDir = $Args[0]
$InstallDir = if ($Args[1] -ne $null) { $Args[1] } else { Join-Path $BuildDir "local/" }

$BuildAll = $env:BUILD_ALL -ne "0"
$BuildDriverManager = ($BuildAll -and (-not ($env:BUILD_DRIVER_MANAGER -eq "0"))) -or ($env:BUILD_DRIVER_MANAGER -eq "1")
$BuildDriverBigQuery = ($BuildAll -and (-not ($env:BUILD_DRIVER_BIGQUERY -eq "0"))) -or ($env:BUILD_DRIVER_BIGQUERY -eq "1")
$BuildDriverFlightSql = ($BuildAll -and (-not ($env:BUILD_DRIVER_FLIGHTSQL -eq "0"))) -or ($env:BUILD_DRIVER_FLIGHTSQL -eq "1")
$BuildDriverPostgreSQL = ($BuildAll -and (-not ($env:BUILD_DRIVER_POSTGRESQL -eq "0"))) -or ($env:BUILD_DRIVER_POSTGRESQL -eq "1")
$BuildDriverSnowflake = ($BuildAll -and (-not ($env:BUILD_DRIVER_SNOWFLAKE -eq "0"))) -or ($env:BUILD_DRIVER_SNOWFLAKE -eq "1")
$BuildDriverSqlite = ($BuildAll -and (-not ($env:BUILD_DRIVER_SQLITE -eq "0"))) -or ($env:BUILD_DRIVER_SQLITE -eq "1")

$env:LD_LIBRARY_PATH += ":$($InstallDir)"
$env:LD_LIBRARY_PATH += ":$($InstallDir)/bin"
$env:LD_LIBRARY_PATH += ":$($InstallDir)/lib"
$env:PATH += ";$($InstallDir)"
$env:PATH += ";$($InstallDir)\bin"
$env:PATH += ";$($InstallDir)\lib"

function Test-Project {
    Push-Location $BuildDir

    $labels = "driver-common"

    if ($BuildDriverManager) {
        $labels += "|driver-manager"
    }
    if ($BuildDriverFlightSql) {
        $labels += "|driver-bigquery"
    }
    if ($BuildDriverFlightSql) {
        $labels += "|driver-flightsql"
    }
    if ($BuildDriverPostgreSQL) {
        $labels += "|driver-postgresql"
    }
    if ($BuildDriverSnowflake) {
        $labels += "|driver-snowflake"
    }
    if ($BuildDriverSqlite) {
        $labels += "|driver-sqlite"
    }

    ctest --output-on-failure --no-tests=error -L "$($labels)"
    if (-not $?) { exit 1 }

    Pop-Location
}

Test-Project
