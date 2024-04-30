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

cmake_minimum_required(VERSION 3.18)
get_filename_component(REPOSITORY_ROOT ".." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${REPOSITORY_ROOT}/c/cmake_modules/")
include(AdbcDefines)
include(BuildUtils)
project(adbc
        VERSION "${ADBC_BASE_VERSION}"
        LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CTest)

add_subdirectory(vendor/fmt EXCLUDE_FROM_ALL)
set_target_properties(fmt PROPERTIES POSITION_INDEPENDENT_CODE ON)
add_subdirectory(vendor/nanoarrow)
add_subdirectory(driver/common)
add_subdirectory(driver/framework)

if(ADBC_BUILD_TESTS)
  add_subdirectory(validation)
endif()

if(ADBC_BUILD_BENCHMARKS)
  add_custom_target(all-benchmarks)
  add_custom_target(benchmark ctest -L benchmark)
  add_dependencies(benchmark all-benchmarks)
endif()

if(ADBC_INTEGRATION_DUCKDB)
  set(ADBC_DRIVER_MANAGER ON)
endif()

if(ADBC_DRIVER_FLIGHTSQL)
  add_subdirectory(driver/flightsql)
endif()

if(ADBC_DRIVER_MANAGER)
  add_subdirectory(driver_manager)
endif()

if(ADBC_DRIVER_POSTGRESQL)
  add_subdirectory(driver/postgresql)
endif()

if(ADBC_DRIVER_SQLITE)
  add_subdirectory(driver/sqlite)
endif()

if(ADBC_DRIVER_SNOWFLAKE)
  add_subdirectory(driver/snowflake)
endif()

if(ADBC_INTEGRATION_DUCKDB)
  add_subdirectory(integration/duckdb)
endif()

if(ADBC_BUILD_PYTHON)
  find_package(Python3 REQUIRED COMPONENTS Interpreter Development)

  if(NOT ADBC_BUILD_SHARED)
    message(FATAL_ERROR "Building Python requires ADBC_BUILD_SHARED=ON")
  endif()

  # NB: the Python packages require the driver manager to be installed,
  # but you don't technically need -DADBC_DRIVER_MANAGER=ON when installing
  # other Python packages. To be safe then, we always install the driver
  # manager package, regardless of the value of -DABC_DRIVER_MANAGER
  # --config-settings eidtable_mode=compat required due to
  # https://github.com/python/mypy/issues/13392
  add_custom_target(python
                    COMMAND ${Python3_EXECUTABLE} -m pip install --no-deps -e
                            "${REPOSITORY_ROOT}/python/adbc_driver_manager"
                            --config-settings editable_mode=compat)

  macro(adbc_install_python_package TARGET)
    string(TOUPPER ${TARGET} ${TARGET}_LIB_upper)
    add_custom_command(TARGET python
                       POST_BUILD
                       COMMAND ${CMAKE_COMMAND} -E env "ADBC_${${TARGET}_LIB_upper}_\
LIBRARY=$<TARGET_FILE:adbc_driver_${TARGET}_shared>" ${Python3_EXECUTABLE} -m pip install
                               --no-deps -e
                               "${REPOSITORY_ROOT}/python/adbc_driver_${TARGET}"
                       COMMENT "pip installing the adbc_driver_${TARGET} library..."
                               DEPENDS $<TARGET_FILE:adbc_driver_${TARGET}_shared>
                       WORKING_DIRECTORY ${REPOSITORY_ROOT})
  endmacro()

  if(ADBC_DRIVER_POSTGRESQL)
    adbc_install_python_package(postgresql)
  endif()

  if(ADBC_DRIVER_SQLITE)
    adbc_install_python_package(sqlite)
  endif()

  if(ADBC_DRIVER_FLIGHTSQL)
    adbc_install_python_package(flightsql)
  endif()

  if(ADBC_DRIVER_SNOWFLAKE)
    adbc_install_python_package(snowflake)
  endif()
endif()

validate_config()
config_summary_message()
