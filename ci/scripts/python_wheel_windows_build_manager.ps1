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

param(
    [Parameter(Mandatory = $true)]
    [string]$SourceDir
)

$ErrorActionPreference = "Stop"
$Component = "adbc_driver_manager"
$ComponentDir = Join-Path $SourceDir "python/$Component"
$TemporaryWheelDir = Join-Path $ComponentDir "temp_wheels"

python -m pip install --upgrade pip delvewheel wheel
if (-not $?) { throw "Failed to install Python build dependencies" }

$env:ADBC_BUILD_TYPE = "debug"

Push-Location $ComponentDir
try {
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue build, $TemporaryWheelDir

    Write-Host "=== Checking $Component version ==="
    python "$Component/_version.py"
    if (-not $?) { throw "Failed to check the $Component version" }

    Write-Host "=== Building $Component wheel ==="
    python -m pip wheel --no-deps -w $TemporaryWheelDir -vvv .
    if (-not $?) { throw "Failed to build the $Component wheel" }

    New-Item -ItemType Directory -Force -Path repaired_wheels | Out-Null
    $Wheels = @(Get-ChildItem -Path "$TemporaryWheelDir/*.whl")
    foreach ($Wheel in $Wheels) {
        Copy-Item $Wheel.FullName repaired_wheels
        python -m delvewheel repair -w repaired_wheels $Wheel.FullName
        if (-not $?) { throw "Failed to repair the $Component wheel" }
    }
} finally {
    Pop-Location
    Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $TemporaryWheelDir
}
