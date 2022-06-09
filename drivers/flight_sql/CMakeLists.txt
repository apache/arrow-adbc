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

cmake_minimum_required(VERSION 3.14)
get_filename_component(REPOSITORY_ROOT "../.." ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${REPOSITORY_ROOT}/cmake_modules/")
include(AdbcDefines)
include(BuildUtils)

project(adbc_driver_flight_sql
        VERSION "${ARROW_BASE_VERSION}"
        LANGUAGES CXX)

find_package(Arrow REQUIRED)
# ARROW-12175: Arrow libraries are not CMake components
get_filename_component(ARROW_CONFIG_PATH ${Arrow_CONFIG} DIRECTORY)
find_package(ArrowFlight REQUIRED HINTS ${ARROW_CONFIG_PATH})
find_package(ArrowFlightSql REQUIRED HINTS ${ARROW_CONFIG_PATH})

add_arrow_lib(adbc_driver_flight_sql
              SOURCES
              flight_sql.cc
              SHARED_LINK_LIBS
              arrow_flight_sql_shared
              arrow_flight_shared
              arrow_shared)
include_directories(SYSTEM ${REPOSITORY_ROOT})

if(ARROW_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_flight_sql_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_flight_sql_static)
endif()

add_test_case(driver_flight_sql_test
              PREFIX
              adbc
              SOURCES
              flight_sql_test.cc
              EXTRA_LINK_LIBS
              ${TEST_LINK_LIBS})

validate_config()
config_summary_message()
