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

# This does the following:

# 1.) Builds each managed library as a nuget package using `dotnet pack`
# 2.) Builds each interop library and packages it in to a nuget using `nuget pack`
# 3.) Copies them all to a single folder
# 4.) Need to run tests using the nuget packages (dotnet test ?)

param (
    [string]$destination=".\nugets"
)

$location = Get-Location

Write-Host "Building Snowflake Go driver"

cd ..\src\Drivers\Snowflake

powershell -ExecutionPolicy Unrestricted -File .\Build-SnowflakeDriver.ps1

cd $location

cd ..

Write-Host "Running dotnet pack"

dotnet pack -o $destination
