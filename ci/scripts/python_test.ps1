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
$BuildDriverPostgres = ($BuildAll -and (-not ($env:BUILD_DRIVER_POSTGRES -eq "0"))) -or ($env:BUILD_DRIVER_POSTGRES -eq "1")
$BuildDriverSqlite = ($BuildAll -and (-not ($env:BUILD_DRIVER_SQLITE -eq "0"))) -or ($env:BUILD_DRIVER_SQLITE -eq "1")

function Build-Subproject {
    $Subproject = $Args[0]
    $SubprojectBuild = Join-Path $SourceDir "python\$($Subproject)"

    echo "============================================================"
    echo "Testing $($Subproject)"
    echo "============================================================"

    python -m pytest -vv $SubprojectBuild
    if (-not $?) { exit 1 }
}

if ($BuildDriverManager) {
    $SqliteDir = Get-Childitem `
      -ErrorAction SilentlyContinue `
      -Path $InstallDir `
      -Recurse `
      -Include "adbc_driver_sqlite.dll","libadbc_driver_sqlite.so" | Split-Path -Parent
    if ($SqliteDir -eq $null) {
        echo "Could not find SQLite driver in $($InstallDir)"
        exit 1
    }
    $env:LD_LIBRARY_PATH += ":$($SqliteDir)"
    $env:PATH += ";$($SqliteDir)"
    Build-Subproject adbc_driver_manager
}
if ($BuildDriverPostgres) {
    Build-Subproject adbc_driver_postgres
}
if ($BuildDriverSqlite) {
    Build-Subproject adbc_driver_sqlite
}
