@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem   http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing,
@rem software distributed under the License is distributed on an
@rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem KIND, either express or implied.  See the License for the
@rem specific language governing permissions and limitations
@rem under the License.

@echo on

set source_dir=%1
set build_dir=%2

echo "=== (%PYTHON_VERSION%) Building ADBC libpq driver ==="

set CMAKE_BUILD_TYPE=release
set CMAKE_GENERATOR=Visual Studio 15 2017 Win64
set CMAKE_UNITY_BUILD=ON
set VCPKG_FEATURE_FLAGS=-manifests
set VCPKG_TARGET_TRIPLET=x64-windows-static

IF NOT DEFINED VCPKG_ROOT (echo "Must set VCPKG_ROOT" && exit /B 1)

%VCPKG_ROOT%\vcpkg install --triplet=%VCPKG_TARGET_TRIPLET% libpq sqlite3

set ADBC_FLIGHTSQL_LIBRARY=%build_dir%\flightsql\adbc_driver_flightsql.dll

mkdir %build_dir%\flightsql
pushd %source_dir%\go\adbc\pkg
go build -tags driverlib -o %ADBC_FLIGHTSQL_LIBRARY% -buildmode=c-shared ./flightsql
popd

mkdir %build_dir%\postgresql
pushd %build_dir%\postgresql

cmake ^
      -G "%CMAKE_GENERATOR%" ^
      -DADBC_BUILD_SHARED=ON ^
      -DADBC_BUILD_STATIC=OFF ^
      -DCMAKE_BUILD_TYPE=%CMAKE_BUILD_TYPE% ^
      -DCMAKE_INSTALL_PREFIX=%build_dir% ^
      -DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake ^
      -DCMAKE_UNITY_BUILD=%CMAKE_UNITY_BUILD% ^
      -DVCPKG_TARGET_TRIPLET=%VCPKG_TARGET_TRIPLET% ^
      %source_dir%\c\driver\postgresql || exit /B 1
cmake --build . --config %CMAKE_BUILD_TYPE% --target install --verbose -j || exit /B 1

@REM XXX: CMake installs it to bin instead of lib for some reason
set ADBC_POSTGRESQL_LIBRARY=%build_dir%\bin\adbc_driver_postgresql.dll

popd

mkdir %build_dir%\sqlite
pushd %build_dir%\sqlite

cmake ^
      -G "%CMAKE_GENERATOR%" ^
      -DADBC_BUILD_SHARED=ON ^
      -DADBC_BUILD_STATIC=OFF ^
      -DCMAKE_BUILD_TYPE=%CMAKE_BUILD_TYPE% ^
      -DCMAKE_INSTALL_PREFIX=%build_dir% ^
      -DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%/scripts/buildsystems/vcpkg.cmake ^
      -DCMAKE_UNITY_BUILD=%CMAKE_UNITY_BUILD% ^
      -DVCPKG_TARGET_TRIPLET=%VCPKG_TARGET_TRIPLET% ^
      %source_dir%\c\driver\sqlite || exit /B 1
cmake --build . --config %CMAKE_BUILD_TYPE% --target install --verbose -j || exit /B 1

@REM XXX: CMake installs it to bin instead of lib for some reason
set ADBC_SQLITE_LIBRARY=%build_dir%\bin\adbc_driver_sqlite.dll

popd

python -m pip install --upgrade pip delvewheel wheel

FOR /F %%i IN ('python -c "import sysconfig; print(sysconfig.get_platform())"') DO set PLAT_NAME=%%i

FOR %%c IN (adbc_driver_manager adbc_driver_flightsql adbc_driver_postgresql adbc_driver_sqlite) DO (
    pushd %source_dir%\python\%%c

    echo "=== (%PYTHON_VERSION%) Checking %%c version ==="
    python %%c\_version.py

    echo "=== (%PYTHON_VERSION%) Building %%c wheel ==="
    python -m pip wheel --no-deps -w dist -vvv . || exit /B 1

    echo "=== (%PYTHON_VERSION%) Re-tag %%c wheel ==="
    FOR %%w IN (dist\*.whl) DO (
        python %source_dir%\ci\scripts\python_wheel_fix_tag.py %%w || exit /B 1
    )

    echo "=== (%PYTHON_VERSION%) Repair %%c wheel ==="
    FOR %%w IN (dist\*.whl) DO (
        delvewheel repair -w repaired_wheels\ %%w
    )

    popd
)
