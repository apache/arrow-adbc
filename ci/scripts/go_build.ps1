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

$ErrorActionPreference = "Stop"

$SourceDir = $Args[0]
$BuildDir = $Args[1]
$InstallDir = if ($Args[2] -ne $null) { $Args[2] } else { Join-Path $BuildDir "local/" }

$GoDir = Join-Path $SourceDir "go" "adbc"

Push-Location $GoDir

go build -v ./...
if (-not $?) { exit 1 }

if ($env:CGO_ENABLED -eq "1") {
    Push-Location pkg
    go build `
      -tags driverlib `
      -o adbc_driver_flightsql.dll `
      -buildmode=c-shared `
      ./flightsql
    if (-not $?) { exit 1 }
    Pop-Location
}

Pop-Location
