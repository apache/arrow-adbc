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
    [ValidateSet("amd64", "arm64")]
    [string]$Architecture
)

$ErrorActionPreference = "Stop"
$Components = @(
    "adbc_driver_manager",
    "adbc_driver_flightsql",
    "adbc_driver_postgresql",
    "adbc_driver_sqlite"
)
$PythonTag = python -c "import sysconfig; print('cp' + sysconfig.get_python_version().replace('.', ''))"
if (-not $?) { throw "Failed to determine the Python tag" }
# https://github.com/python/cpython/issues/127405: unlike Unix, Windows does not
# expose free-threading through abiflags, so use Py_GIL_DISABLED instead.
$PythonFlags = python -c "import sysconfig; print('t' if sysconfig.get_config_var('Py_GIL_DISABLED') else '')"
if (-not $?) { throw "Failed to determine whether Python is free-threaded" }
$PythonTag = $PythonTag.Trim()
$PythonFlags = $PythonFlags.Trim()
if ($PythonFlags.Contains("t")) {
    $env:PYTHON_GIL = "0"
}

Write-Host "=== ($PythonTag$PythonFlags) Installing wheels ==="

foreach ($Component in $Components) {
    if ($Component -eq "adbc_driver_manager") {
        $WheelPattern = "*-$PythonTag-$PythonTag$PythonFlags-*.whl"
    } else {
        $WheelPattern = "*-py3-none-*.whl"
    }

    $WheelDir = Join-Path $SourceDir "python/$Component/repaired_wheels"
    $Wheels = @(Get-ChildItem -Path (Join-Path $WheelDir $WheelPattern))
    if ($Wheels.Count -ne 1) {
        throw "Expected one $Component wheel matching $WheelPattern, found $($Wheels.Count)"
    }

    python -m pip install --no-deps --force-reinstall $Wheels[0].FullName
    if (-not $?) { throw "Failed to install the $Component wheel" }
}

python -m pip install pytest typing-extensions
if (-not $?) { throw "Failed to install Python test dependencies" }

foreach ($Component in $Components) {
    python -c "import $Component; import $Component.dbapi"
    if (-not $?) { throw "Failed to import $Component" }
}

if ($PythonFlags.Contains("t")) {
    Write-Host "=== Testing free-threaded driver manager without PyArrow or Polars ==="
    python -m pytest -vvx --import-mode append `
        -k "unknown_driver or missing_platform or bad" `
        (Join-Path $SourceDir "python/adbc_driver_manager/tests/test_manifest.py")
    if (-not $?) { throw "Failed to test the free-threaded driver manager" }
} elseif ($Architecture -eq "amd64") {
    python -m pip install pyarrow pandas protobuf
    if (-not $?) { throw "Failed to install amd64 Python test dependencies" }

    foreach ($Component in $Components) {
        Write-Host "=== Testing $Component ==="
        python -m pytest -vvx --import-mode append `
            -k "not duckdb and not sqlite and not polars" `
            (Join-Path $SourceDir "python/$Component/tests")
        if (-not $?) { throw "Failed to test $Component" }
    }
} else {
    python -m pip install polars
    if (-not $?) { throw "Failed to install Polars" }

    Write-Host "=== Testing driver manager with Polars and without PyArrow ==="
    $env:ADBC_NO_SKIP_TESTS = "1"
    python -m pytest -vvx --import-mode append `
        (Join-Path $SourceDir "python/adbc_driver_manager/tests/test_dbapi_polars_nopyarrow.py")
    if (-not $?) { throw "Failed to test the driver manager with Polars" }
}
