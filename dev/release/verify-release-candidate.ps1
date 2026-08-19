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

  Verify only the wheels:
    `$env:TEST_DEFAULT = "0"
    `$env:TEST_WHEELS = "1"
    $($script) X.Y.Z RC_NUMBER

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
$TestWheels = Get-Bool "TEST_WHEELS" $TestBinaries
$TestBinaryArtifacts = $TestJars -or $TestWheels

if (-not $TestSource -and -not $TestBinaryArtifacts) {
    echo "Nothing to test, exiting"
    exit 1
}

echo "Default: $($TestDefault)"
echo "Source: $($TestSource)"
echo "Binaries: $($TestBinaries)"
echo "- JARs: $($TestJars)"
echo "- Wheels: $($TestWheels)"

function Enable-Conda {
    if (-not $script:CondaInitialized) {
        Invoke-Expression $(conda shell.powershell hook | Out-String)
        $script:CondaInitialized = $true
    }
}

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

$AdbcSourceDir = Join-Path $PSScriptRoot "..\.." | Resolve-Path | % { $_.Path }

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
if ($TestBinaryArtifacts) {
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

    $PlatformSpecificPackages = @()
    if ($env:OS -eq "Windows_NT") {
        $PlatformSpecificPackages += "m2w64-gcc"
    }
    mamba create -c conda-forge --yes --prefix $(Join-Path $ArrowTempDir conda-env) `
      --file $(Join-Path $ArrowSourceDir ci\conda_env_cpp.txt) `
      --file $(Join-Path $ArrowSourceDir ci\conda_env_python.txt) `
      go `
      $PlatformSpecificPackages

    Enable-Conda
    conda activate $(Join-Path $ArrowTempDir conda-env)
    # XXX: force bundled gtest as the conda-forge version appears to require you
    # to exactly match the MSVC version it was compiled with.  Uninstalling also
    # removes a bunch of other things, so force-remove instead
    # (https://github.com/conda-forge/libprotobuf-feedstock/issues/186)
    # Use conda, mamba appears to ignore --force
    conda remove -y --force gtest

    # Activating doesn't appear to set GOROOT
    $env:GOROOT = $(Join-Path $ArrowTempDir $(Join-Path conda-env go))

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

if ($TestBinaryArtifacts) {
    Show-Header "Verify Binary Distribution"

    if ($TestWheels) {
        Show-Header "Verify Python Wheels"

        $IsWindowsPlatform = $env:OS -eq "Windows_NT"
        if ($IsWindowsPlatform) {
            if ($env:PROCESSOR_ARCHITECTURE -ne "AMD64") {
                throw "Unsupported Windows architecture: $($env:PROCESSOR_ARCHITECTURE)"
            }
            $Platform = "windows"
            $PlatformPattern = "win_amd64"
            $Architecture = "amd64"
        } else {
            $Kernel = (uname -s)
            $Machine = (uname -m)
            if ($Kernel -eq "Darwin") {
                $Platform = "macosx"
                if ($Machine -eq "arm64") {
                    $Architecture = "arm64"
                } else {
                    $Architecture = "x86_64"
                }
            } elseif ($Kernel -eq "Linux") {
                $Platform = "manylinux"
                if ($Machine -eq "aarch64") {
                    $Architecture = "aarch64"
                } else {
                    $Architecture = "x86_64"
                }
            } else {
                throw "Unsupported platform: $($Kernel)"
            }
            $PlatformPattern = "$($Platform)*$($Architecture)*"
        }

        Enable-Conda

        if ($env:TEST_PYTHON_VERSIONS -eq $null) {
            $PythonVersions = @("3.10", "3.11", "3.12", "3.13", "3.14", "3.14t")
        } else {
            $PythonVersions = @($env:TEST_PYTHON_VERSIONS.Split(" ", [System.StringSplitOptions]::RemoveEmptyEntries))
        }

        $Components = @(
            "adbc_driver_manager",
            "adbc_driver_flightsql",
            "adbc_driver_postgresql",
            "adbc_driver_sqlite"
        )

        foreach ($PythonVersion in $PythonVersions) {
            Show-Header "Verify Python $($PythonVersion) Wheels for $($Platform)/$($Architecture)"

            $FreeThreaded = $PythonVersion.EndsWith("t")
            $CondaPythonVersion = $PythonVersion.TrimEnd("t")
            $PythonTag = "cp$($CondaPythonVersion.Replace('.', ''))"
            if ($FreeThreaded) {
                $AbiTag = "$($PythonTag)t"
            } else {
                $AbiTag = $PythonTag
            }

            $CondaEnv = Join-Path $ArrowTempDir "wheel-$($PythonVersion)-$($Platform)-$($Architecture)"
            if ($IsWindowsPlatform) {
                $CondaPython = Join-Path $CondaEnv "python.exe"
            } else {
                $CondaPython = Join-Path $CondaEnv "bin/python"
            }
            if (-not (Test-Path -Path $CondaPython -PathType Leaf)) {
                $CondaPackages = @("python=$($CondaPythonVersion)")
                if ($FreeThreaded) {
                    $CondaPackages += "python-freethreading"
                }
                mamba create -c conda-forge --yes --prefix $CondaEnv $CondaPackages
                if (-not $?) { throw "Failed to create Python $($PythonVersion) Conda environment" }
            } else {
                echo "Using cached $($CondaEnv)"
            }
            conda activate $CondaEnv

            $WheelPaths = @()
            foreach ($Component in $Components) {
                if ($Component -eq "adbc_driver_manager") {
                    $WheelPattern = "$($Component)-*-$($PythonTag)-$($AbiTag)-$($PlatformPattern).whl"
                } else {
                    $WheelPattern = "$($Component)-*-py3-none-$($PlatformPattern).whl"
                }
                $MatchingWheels = @(Get-ChildItem -Path $BinaryDir -Filter $WheelPattern)
                if ($MatchingWheels.Count -ne 1) {
                    throw "Expected exactly one $($Component) wheel matching $($WheelPattern), found $($MatchingWheels.Count)"
                }
                $WheelPaths += $MatchingWheels[0].FullName
            }

            python -m pip install --force-reinstall $WheelPaths
            if (-not $?) { throw "Failed to install Python $($PythonVersion) wheels" }

            $env:PYTHON_VERSION = $PythonVersion
            if ($FreeThreaded) {
                $env:PYTHON_GIL = "0"
            } else {
                Remove-Item -Path Env:PYTHON_GIL -ErrorAction SilentlyContinue
            }

            if ($IsWindowsPlatform) {
                python -m pip install pytest pyarrow pandas protobuf
                if (-not $?) { throw "Failed to install Python $($PythonVersion) wheel test dependencies" }

                foreach ($Component in $Components) {
                    echo "Testing $($Component)"
                    python -c "import $($Component)"
                    if (-not $?) { throw "Failed to import $($Component) with Python $($PythonVersion)" }
                    python -c "import $($Component).dbapi"
                    if (-not $?) { throw "Failed to import $($Component).dbapi with Python $($PythonVersion)" }
                    $TestsPath = Join-Path $AdbcSourceDir "python\$($Component)\tests"
                    python -m pytest -vvx --import-mode=append -k "not duckdb and not sqlite and not polars" $TestsPath
                    if (-not $?) { throw "Failed to test $($Component) with Python $($PythonVersion)" }
                }
            } else {
                & $(Join-Path $AdbcSourceDir "ci/scripts/python_wheel_unix_test.sh") $AdbcSourceDir
                if (-not $?) { throw "Failed to test wheels with Python $($PythonVersion)" }
            }
        }
        Remove-Item -Path Env:PYTHON_GIL -ErrorAction SilentlyContinue
    } else {
        Show-Header "Skipping Python Wheels"
    }

    if ($TestJars) {
        Show-Header "Verify Java JARs"
        if ($env:JAVA_HOME -eq $null) {
            # Work around PowerShell < 7. Temporarily set ErrorActionPreference
            # to continue to avoid the redirect below from stopping the script.
            $PreviousErrorActionPreference = $ErrorActionPreference
            $ErrorActionPreference = "Continue"
            $env:JAVA_HOME = & java -XshowSettings:properties -version 2>&1 | Select-String "java.home" | ForEach-Object { $_.ToString().Split("=")[1].Trim() }
            $ErrorActionPreference = $PreviousErrorActionPreference
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
