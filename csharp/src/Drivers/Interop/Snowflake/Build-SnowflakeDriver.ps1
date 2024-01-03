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

Write-Host "Building the Snowflake ADBC Go driver"

$location = Get-Location

$file = "libadbc_driver_snowflake.dll"

cd ..\..\..\..\..\go\adbc\pkg

if(Test-Path $file)
{
    #because each framework build will run the script, avoid building it each time
    $diff=((ls $file).LastWriteTime - (Get-Date)).TotalSeconds
    if ($diff -gt -30)
    {
        Write-Output "Skipping build of $file because it is too recent"
        exit
    }
}
make $file
COPY $file $location
