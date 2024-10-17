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

Write-Host "Building the Flight SQL ADBC Go driver"
Write-Host "IsPackagingPipeline=$Env:IsPackagingPipeline"

if (-not (Test-Path env:IsPackagingPipeline)) {
    Write-Host "IsPackagingPipeline environment variable does not exist."
    exit
}

# Get the value of the IsPackagingPipeline environment variable
$IsPackagingPipelineValue = $env:IsPackagingPipeline

# Check if the value is "true"
if ($IsPackagingPipelineValue -ne "true") {
    Write-Host "IsPackagingPipeline is not set to 'true'. Exiting the script."
    exit
}

$location = Get-Location

$file = "libadbc_driver_flightsql.dll"

if(Test-Path $file)
{
    exit
}

cd ..\..\..\..\..\go\adbc\pkg

make $file

if(Test-Path $file)
{
    $processes = Get-Process | Where-Object { $_.Modules.ModuleName -contains $file }

    if ($processes.Count -eq 0) {
        try {
        # File is not being used, copy it to the destination
            Copy-Item -Path $file -Destination $location
            Write-Host "File copied successfully."
        }
        catch {
            Write-Host "Caught error: $_"
        }
    } else {
        Write-Host "File is being used by another process. Cannot copy."
    }
}
