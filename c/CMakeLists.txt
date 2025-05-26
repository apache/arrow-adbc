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
set(CMAKE_C_STANDARD 99)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(CTest)

set(ADBC_TARGET_COMPILE_DEFINITIONS)
if(NOT ADBC_DEFINE_COMMON_ENTRYPOINTS)
  message(STATUS "Defining ADBC_NO_COMMON_ENTRYPOINTS")
  set(ADBC_TARGET_COMPILE_DEFINITIONS "ADBC_NO_COMMON_ENTRYPOINTS")
endif()

if(ADBC_WITH_VENDORED_FMT)
  add_subdirectory(vendor/fmt EXCLUDE_FROM_ALL)
  set_target_properties(fmt PROPERTIES POSITION_INDEPENDENT_CODE ON)
else()
  find_package(fmt REQUIRED)
endif()
if(ADBC_WITH_VENDORED_NANOARROW)
  add_subdirectory(vendor/nanoarrow)
else()
  find_package(nanoarrow REQUIRED)
endif()
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
  install(FILES "${REPOSITORY_ROOT}/c/include/arrow-adbc/driver/flightsql.h"
          DESTINATION include/arrow-adbc/driver)
  add_subdirectory(driver/flightsql)
endif()

if(ADBC_DRIVER_MANAGER)
  install(FILES "${REPOSITORY_ROOT}/c/include/adbc_driver_manager.h" DESTINATION include)
  install(FILES "${REPOSITORY_ROOT}/c/include/arrow-adbc/adbc_driver_manager.h"
          DESTINATION include/arrow-adbc)
  add_subdirectory(driver_manager)
endif()

if(ADBC_DRIVER_POSTGRESQL)
  install(FILES "${REPOSITORY_ROOT}/c/include/arrow-adbc/driver/postgresql.h"
          DESTINATION include/arrow-adbc/driver)
  add_subdirectory(driver/postgresql)
endif()

if(ADBC_DRIVER_SQLITE)
  install(FILES "${REPOSITORY_ROOT}/c/include/arrow-adbc/driver/sqlite.h"
          DESTINATION include/arrow-adbc/driver)
  add_subdirectory(driver/sqlite)
endif()

if(ADBC_DRIVER_SNOWFLAKE)
  install(FILES "${REPOSITORY_ROOT}/c/include/arrow-adbc/driver/snowflake.h"
          DESTINATION include/arrow-adbc/driver)
  add_subdirectory(driver/snowflake)
endif()

if(ADBC_DRIVER_BIGQUERY)
  install(FILES "${REPOSITORY_ROOT}/c/include/arrow-adbc/driver/bigquery.h"
          DESTINATION include/arrow-adbc/driver)
  add_subdirectory(driver/bigquery)
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
  add_custom_target(python COMMAND ${Python3_EXECUTABLE} -m pip install --no-deps -e
                                   "${REPOSITORY_ROOT}/python/adbc_driver_manager")

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

  if(ADBC_DRIVER_BIGQUERY)
    adbc_install_python_package(bigquery)
  endif()
endif()

validate_config()
config_summary_message()
