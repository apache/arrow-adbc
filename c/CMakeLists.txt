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

include(CTest)

add_subdirectory(vendor/nanoarrow)
add_subdirectory(driver/common)

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

validate_config()
config_summary_message()
