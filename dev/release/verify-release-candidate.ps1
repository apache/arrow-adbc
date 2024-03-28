#!/usr/bin/env pwsh
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

# Expects PowerShell 7 but also tested with PowerShell 5.1 (built into
# Windows 7+). Works on Linux!

# To reuse build artifacts between runs set ARROW_TMPDIR environment variable to
# a directory where the temporary files should be placed to, note that this
# directory is not cleaned up automatically.

$ErrorActionPreference = "Stop"

if ($env:VERBOSE) {
    Set-PSDebug -Trace 1
}

if ($args.Count -eq 0) {
    $Version = "HEAD"
    $SourceKind = "local"
} elseif ($args.Count -eq 2) {
    $Version = $args[0]
    $RcNumber = $args[1]
    $SourceKind = "tarball"
} else {
    $script = Split-Path $PSCommandPath -leaf
    echo @"
Usage:
  Verify release candidate:
    $($script) X.Y.Z RC_NUMBER
  Verify current checkout:
    $($script)

Assumes Mamba is set up and available on the path.
"@
    exit 1
}

$ArrowDistUrl = "https://dist.apache.org/repos/dist/dev/arrow"
$DistName = "apache-arrow-adbc-$($Version)"

function Download-Dist-File {
    $DistUrl = "$($ArrowDistUrl)/$($DistName)-rc$($RcNumber)/$($args[0])"
    $DistPath = Join-Path $ArrowTempDir $args[0]

    echo "Fetching $($DistUrl)"
    if ($env:VERIFICATION_MOCK_DIST_DIR -eq $null) {
        Invoke-WebRequest -Uri $DistUrl -OutFile $DistPath
    } else {
        $SourcePath = Join-Path $env:VERIFICATION_MOCK_DIST_DIR $args[0]
        cp $SourcePath $DistPath
    }
}

function Show-Header {
    echo ""
    echo "============================================================"
    echo $args[0]
    echo "============================================================"
}

Show-Header "Create Temporary Directory"
if ($env:ARROW_TMPDIR -eq $null) {
    $ArrowTempDir = New-TemporaryFile | % { $_.FullName }
    Remove-Item -Force $ArrowTempDir
} else {
    $ArrowTempDir = $env:ARROW_TMPDIR
}
New-Item -ItemType Directory -Force -Path $ArrowTempDir | Out-Null

echo "Using $($ArrowTempDir)"

Show-Header "Ensure Source Directory"

if ($SourceKind -eq "local") {
    $ArrowSourceDir = Join-Path $PSScriptRoot "..\.." | Resolve-Path | % { $_.Path }
} else {
    $ArrowSourceDir = Join-Path $ArrowTempDir $DistName
    New-Item -ItemType Directory -Path $ArrowSourceDir -Force
    # Convert to an absolute now that it should exist
    $ArrowSourceDir = $ArrowSourceDir | Resolve-Path | % { $_.Path }

    Download-Dist-File "$($DistName).tar.gz"
    Download-Dist-File "$($DistName).tar.gz.sha512"

    $DistPath = Join-Path $ArrowTempDir "$($DistName).tar.gz"
    $Sha512Path = Join-Path $ArrowTempDir "$($DistName).tar.gz.sha512"

    $ExpectedSha512 = (Get-Content $Sha512Path).Split(" ")[0]
    if (-not ((Get-FileHash -Algorithm SHA512 $DistPath).Hash -eq $ExpectedSha512)) {
        echo "SHA512 hash mismatch"
        exit 1
    }

    tar -C $ArrowSourceDir --strip-components 1 -xf $DistPath
}

echo "Using $($ArrowSourceDir)"

Show-Header "Create Conda Environment"

mamba create -c conda-forge -f -y -p $(Join-Path $ArrowTempDir conda-env) `
  --file $(Join-Path $ArrowSourceDir ci\conda_env_cpp.txt) `
  --file $(Join-Path $ArrowSourceDir ci\conda_env_python.txt) `
  go `
  m2w64-gcc

Invoke-Expression $(conda shell.powershell hook | Out-String)
conda activate $(Join-Path $ArrowTempDir conda-env)
# XXX: force bundled gtest as the conda-forge version appears to require you
# to exactly match the MSVC version it was compiled with.  Uninstalling also
# removes a bunch of other things, so force-remove instead
# (https://github.com/conda-forge/libprotobuf-feedstock/issues/186)
# Use conda, mamba appears to ignore --force
conda remove -y --force gtest

# Activating doesn't appear to set GOROOT
$env:GOROOT = $(Join-Path $ArrowTempDir conda-env go)

Show-Header "Verify C/C++ Sources"

$CppBuildDir = Join-Path $ArrowTempDir cpp-build
New-Item -ItemType Directory -Force -Path $CppBuildDir | Out-Null

# XXX(apache/arrow-adbc#634): not working on Windows due to it picking
# up MSVC as the C compiler, which then blows up when /Werror gets
# passed in by some package
$env:BUILD_DRIVER_FLIGHTSQL = "0"
$env:BUILD_DRIVER_SNOWFLAKE = "0"

& $(Join-Path $ArrowSourceDir ci\scripts\cpp_build.ps1) $ArrowSourceDir $CppBuildDir
if (-not $?) { exit 1 }

$env:BUILD_DRIVER_POSTGRESQL = "0"
& $(Join-Path $ArrowSourceDir ci\scripts\cpp_test.ps1) $CppBuildDir
if (-not $?) { exit 1 }
$env:BUILD_DRIVER_POSTGRESQL = "1"

Show-Header "Verify Python Sources"

& $(Join-Path $ArrowSourceDir ci\scripts\python_build.ps1) $ArrowSourceDir $CppBuildDir
if (-not $?) { exit 1 }

& $(Join-Path $ArrowSourceDir ci\scripts\python_test.ps1) $ArrowSourceDir $CppBuildDir
if (-not $?) { exit 1 }

Show-Header "Release candidate looks good!"
