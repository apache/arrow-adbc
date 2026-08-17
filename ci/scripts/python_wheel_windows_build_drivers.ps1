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
    [string]$SourceDir,
    [Parameter(Mandatory = $true)]
    [string]$BuildDir,
    [Parameter(Mandatory = $true)]
    [ValidateSet("amd64", "arm64")]
    [string]$Architecture
)

$ErrorActionPreference = "Stop"

if (-not $env:VCPKG_ROOT) {
    throw "Must set VCPKG_ROOT"
}

if ($Architecture -eq "amd64") {
    $CMakePlatform = "x64"
    $PlatformTag = "win_amd64"
    $VcpkgTriplet = "x64-windows-static"
} else {
    $CMakePlatform = "ARM64"
    $PlatformTag = "win_arm64"
    $VcpkgTriplet = "arm64-windows-static"
}

$BuildType = "RelWithDebInfo"
$BuildStatic = if ($env:ADBC_BUILD_STATIC) { $env:ADBC_BUILD_STATIC } else { "OFF" }
$env:VCPKG_FEATURE_FLAGS = "-manifests"
$env:VCPKG_TARGET_TRIPLET = $VcpkgTriplet
$env:VCPKG_DEFAULT_HOST_TRIPLET = $VcpkgTriplet

Write-Host "=== Building ADBC drivers for $Architecture ==="

& (Join-Path $env:VCPKG_ROOT "vcpkg.exe") install `
    "--triplet=$VcpkgTriplet" `
    "libpq" `
    "sqlite3[dbstat,fts3,fts4,fts5,geopoly,json1,limit,math,rtree,session,snapshot,soundex]"
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

New-Item -ItemType Directory -Force -Path $BuildDir | Out-Null

cmake `
    -S (Join-Path $SourceDir "c") `
    -B $BuildDir `
    -G "Visual Studio 18 2026" `
    -A $CMakePlatform `
    -DADBC_BUILD_SHARED=ON `
    "-DADBC_BUILD_STATIC=$BuildStatic" `
    "-DCMAKE_BUILD_TYPE=$BuildType" `
    "-DCMAKE_INSTALL_PREFIX=$BuildDir" `
    "-DCMAKE_TOOLCHAIN_FILE=$(Join-Path $env:VCPKG_ROOT 'scripts/buildsystems/vcpkg.cmake')" `
    -DCMAKE_UNITY_BUILD=ON `
    "-DVCPKG_TARGET_TRIPLET=$VcpkgTriplet" `
    -DADBC_DRIVER_FLIGHTSQL=ON `
    -DADBC_DRIVER_MANAGER=ON `
    -DADBC_DRIVER_POSTGRESQL=ON `
    -DADBC_DRIVER_SQLITE=ON
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

cmake --build $BuildDir --config $BuildType --target install --verbose -j
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$env:ADBC_FLIGHTSQL_LIBRARY = Join-Path $BuildDir "bin/adbc_driver_flightsql.dll"
$env:ADBC_POSTGRESQL_LIBRARY = Join-Path $BuildDir "bin/adbc_driver_postgresql.dll"
$env:ADBC_SQLITE_LIBRARY = Join-Path $BuildDir "bin/adbc_driver_sqlite.dll"

python -m pip install --upgrade pip delvewheel wheel
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$env:ADBC_BUILD_TYPE = "debug"
$Components = @(
    "adbc_driver_flightsql",
    "adbc_driver_postgresql",
    "adbc_driver_sqlite"
)

foreach ($Component in $Components) {
    $ComponentDir = Join-Path $SourceDir "python/$Component"
    Push-Location $ComponentDir
    try {
        Remove-Item -Recurse -Force -ErrorAction SilentlyContinue build, dist, repaired_wheels

        Write-Host "=== Checking $Component version ==="
        python "$Component/_version.py"
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

        Write-Host "=== Building $Component wheel ==="
        python -m pip wheel --no-deps -w dist -vvv .
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

        $Wheels = @(Get-ChildItem -Path "dist/*.whl")
        foreach ($Wheel in $Wheels) {
            python (Join-Path $SourceDir "ci/scripts/python_wheel_fix_tag.py") `
                "--plat-name=$PlatformTag" $Wheel.FullName
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }

        New-Item -ItemType Directory -Force -Path repaired_wheels | Out-Null
        $Wheels = @(Get-ChildItem -Path "dist/*.whl")
        foreach ($Wheel in $Wheels) {
            Copy-Item $Wheel.FullName repaired_wheels
            python -m delvewheel repair -w repaired_wheels $Wheel.FullName
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }
    } finally {
        Pop-Location
    }
}
