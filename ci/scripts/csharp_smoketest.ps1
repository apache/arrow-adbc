#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

param (
    [string]$destination=".\packages"
)

$ErrorActionPreference = "Stop"

Write-Host "This script performs the following steps:"
Write-Host "  - Runs unit tests against all projects"
Write-Host "  - Packages everything to NuGet packages in $destination"
Write-Host "  - Runs smoke tests using the NuGet packages"

Write-Host ""

cd $PSScriptRoot
cd ..\..\csharp

Write-Host "Running dotnet test"

dotnet test

Write-Host "Running dotnet pack"

$loc = Get-Location

Write-Host $loc

Invoke-Expression "powershell -executionpolicy bypass -File $PSScriptRoot\csharp_pack.ps1 -destination $destination"

Write-Host "Running smoke tests"

cd test\SmokeTests

dotnet test
