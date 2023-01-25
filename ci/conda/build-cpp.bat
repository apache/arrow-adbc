REM Licensed to the Apache Software Foundation (ASF) under one
REM or more contributor license agreements.  See the NOTICE file
REM distributed with this work for additional information
REM regarding copyright ownership.  The ASF licenses this file
REM to you under the Apache License, Version 2.0 (the
REM "License"); you may not use this file except in compliance
REM with the License.  You may obtain a copy of the License at
REM
REM   http://www.apache.org/licenses/LICENSE-2.0
REM
REM Unless required by applicable law or agreed to in writing,
REM software distributed under the License is distributed on an
REM "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
REM KIND, either express or implied.  See the License for the
REM specific language governing permissions and limitations
REM under the License.

if "%PKG_NAME%" == "adbc-driver-manager-cpp" (
    set PKG_ROOT=c\driver_manager
    goto BUILD
)
if "%PKG_NAME%" == "adbc-driver-flightsql-go" (
    set PKG_ROOT=c\driver\flightsql
    goto BUILD
)
if "%PKG_NAME%" == "adbc-driver-postgresql-cpp" (
    set PKG_ROOT=c\driver\postgresql
    goto BUILD
)
if "%PKG_NAME%" == "adbc-driver-sqlite-cpp" (
    set PKG_ROOT=c\driver\sqlite
    goto BUILD
)
echo Unknown package %PKG_NAME%
exit 1

:BUILD

mkdir "%SRC_DIR%"\build-cpp\%PKG_NAME%
pushd "%SRC_DIR%"\build-cpp\%PKG_NAME%

cmake ..\..\%PKG_ROOT% ^
      -G Ninja ^
      -DADBC_BUILD_SHARED=ON ^
      -DADBC_BUILD_STATIC=OFF ^
      -DCMAKE_INSTALL_PREFIX=%LIBRARY_PREFIX% ^
      -DCMAKE_PREFIX_PATH=%PREFIX% ^
      || exit /B 1

cmake --build . --target install --config Release -j || exit /B 1

popd
