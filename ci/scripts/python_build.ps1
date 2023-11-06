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

$ErrorActionPreference = "Stop"

$SourceDir = $Args[0]
$BuildDir = $Args[1]

$BuildAll = $env:BUILD_ALL -ne "0"
$BuildDriverManager = ($BuildAll -and (-not ($env:BUILD_DRIVER_MANAGER -eq "0"))) -or ($env:BUILD_DRIVER_MANAGER -eq "1")

cmake -S "$($SourceDir)\c" -B $BuildDir -DADBC_DRIVER_MANAGER=$BuildDriverManager -DADBC_BUILD_PYTHON=ON
cmake --build build --target python
