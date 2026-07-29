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

function Show-Header {
    echo ""
    echo "============================================================"
    echo $args[0]
    echo "============================================================"
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

# ============================================================
# What to test
# ============================================================

function Get-Bool($envname, [bool] $default) {
    if (-not (Test-Path -Path "Env:$envname")) {
        return $default
    }

    $value = (Get-Item -Path "Env:$envname").Value
    switch ($value) {
        "1" { return $true }
        "0" { return $false }
        "true" { return $true }
        "false" { return $false }
        "on" { return $true }
        "off" { return $false }
        "yes" { return $true }
        "no" { return $false }
        default { throw "Invalid boolean value for $($envname): $($value)" }
    }
}

$TestDefault = Get-Bool "TEST_DEFAULT" $true
$TestSource = Get-Bool "TEST_SOURCE" $TestDefault
$TestBinaries = Get-Bool "TEST_BINARIES" $TestDefault
$TestBinaries = $TestBinaries -and ($SourceKind -eq "tarball")
$TestJars = Get-Bool "TEST_JARS" $TestBinaries

if (-not $TestSource -and -not $TestBinaries) {
    echo "Nothing to test, exiting"
    exit 1
}

echo "Default: $($TestDefault)"
echo "Source: $($TestSource)"
echo "Binaries: $($TestBinaries)"
echo "- JARs: $($TestJars)"

# ============================================================
# Set up common artifacts
# ============================================================

Show-Header "Create Temporary Directory"
if ($env:ARROW_TMPDIR -eq $null) {
    $ArrowTempDir = New-TemporaryFile | % { $_.FullName }
    Remove-Item -Force $ArrowTempDir
} else {
    $ArrowTempDir = $env:ARROW_TMPDIR
}
New-Item -ItemType Directory -Force -Path $ArrowTempDir | Out-Null
echo "Using $($ArrowTempDir)"

Show-Header "Clone apache/arrow"
$ArrowSourceDir = Join-Path $ArrowTempDir "arrow"
$StampFile = Join-Path $ArrowSourceDir "stamp.txt"
if (-not (Test-Path -Path $StampFile)) {
    git clone --depth 1 https://github.com/apache/arrow $ArrowSourceDir
    if (-not $?) { throw "Failed to clone apache/arrow" }
    New-Item -ItemType File -Force -Path $StampFile | Out-Null
} else {
    echo "Using cached $($ArrowSourceDir)"
}

$BinaryDir = Join-Path $ArrowTempDir "binaries"
if ($TestBinaries) {
    Show-Header "Download binary artifacts"

    $StampFile = Join-Path $BinaryDir "stamp.txt"
    if (-not (Test-Path -Path $StampFile)) {
        python "$ArrowSourceDir/dev/release/download_rc_binaries.py" `
               $Version $RcNumber `
               --dest="$BinaryDir" `
               --num_parallel 4 `
               --package_type=github `
               --repository="apache/arrow-adbc" `
               --tag="apache-arrow-adbc-$($Version)-rc$($RcNumber)"
        if (-not $?) { throw "Failed to download binary artifacts" }
        New-Item -ItemType File -Force -Path $StampFile | Out-Null
    } else {
        echo "Using cached $($BinaryDir)"
    }
}


# ============================================================
# Test release
# ============================================================
if ($TestSource) {
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

    mamba create -c conda-forge --yes --prefix $(Join-Path $ArrowTempDir conda-env) `
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

    $env:_ADBC_IS_CONDA = "1"
    # XXX(apache/arrow-adbc#634): not working on Windows due to it picking
    # up MSVC as the C compiler, which then blows up when /Werror gets
    # passed in by some package
    $env:BUILD_DRIVER_FLIGHTSQL = "0"

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
}

if ($TestBinaries) {
    Show-Header "Verify Binary Distribution"

    if ($TestJars) {
        Show-Header "Verify Java JARs"
        if ($env:JAVA_HOME -eq $null) {
            $env:JAVA_HOME = & java -XshowSettings:properties -version 2>&1 | Select-String "java.home" | ForEach-Object { $_.ToString().Split("=")[1].Trim() }
        }
        echo "JAVA_HOME: $($env:JAVA_HOME)"

        $RootPoms = @(Get-ChildItem -Path $BinaryDir -Filter "arrow-adbc-java-root-*.pom")
        if ($RootPoms.Count -ne 1) {
            throw "Expected exactly one Arrow ADBC Java root POM, found $($RootPoms.Count)"
        }

        $RootPomPath = $RootPoms[0].FullName
        [xml] $RootPom = Get-Content -Raw $RootPomPath
        $JavaVersion = $RootPom.project.version
        echo "ADBC version: $($JavaVersion)"

        $MavenRepository = Join-Path $ArrowTempDir "maven-repository"
        New-Item -ItemType Directory -Force -Path $MavenRepository | Out-Null
        $MavenRepositoryArgument = "-Dmaven.repo.local=$($MavenRepository)"

        mvn -B install:install-file `
            $MavenRepositoryArgument `
            "-Dfile=$($RootPomPath)" `
            "-DpomFile=$($RootPomPath)" `
            "-Dpackaging=pom"
        if (-not $?) { exit 1 }

        $Artifacts = @(
            "adbc-core",
            "adbc-driver-flight-sql",
            "adbc-driver-jdbc",
            "adbc-driver-jni",
            "adbc-driver-manager",
            "adbc-sql"
        )
        foreach ($Artifact in $Artifacts) {
            $ArtifactBase = Join-Path $BinaryDir "$($Artifact)-$($JavaVersion)"
            $JarPath = "$($ArtifactBase).jar"
            $PomPath = "$($ArtifactBase).pom"
            $SourcesPath = "$($ArtifactBase)-sources.jar"
            $JavadocPath = "$($ArtifactBase)-javadoc.jar"
            foreach ($Path in @($JarPath, $PomPath, $SourcesPath, $JavadocPath)) {
                if (-not (Test-Path -Path $Path -PathType Leaf)) {
                    throw "Missing Java artifact: $($Path)"
                }
            }

            mvn -B install:install-file `
                $MavenRepositoryArgument `
                "-Dfile=$($JarPath)" `
                "-DpomFile=$($PomPath)" `
                "-Dsources=$($SourcesPath)" `
                "-Djavadoc=$($JavadocPath)"
            if (-not $?) { exit 1 }
        }

        $JavaVerificationPom = Join-Path $PSScriptRoot "verify\java\pom.xml"
        mvn -B test `
            $MavenRepositoryArgument `
            "-Dadbc.version=$($JavaVersion)" `
            -f $JavaVerificationPom
        if (-not $?) { exit 1 }
    } else {
        Show-Header "Skipping Java JARs"
    }
}

Show-Header "Release candidate looks good!"
