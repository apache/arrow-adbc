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
get_filename_component(REPOSITORY_ROOT "../../../" ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${REPOSITORY_ROOT}/c/cmake_modules/")
include(AdbcDefines)
include(BuildUtils)
include(GoUtils)

project(adbc_driver_flightsql VERSION "${ADBC_BASE_VERSION}")
include(CTest)

set(LDFLAGS "$<$<CONFIG:Release>:-s> $<$<CONFIG:Release>:-w>")
add_go_lib("${REPOSITORY_ROOT}/go/adbc/pkg/flightsql/"
                  adbc_driver_flightsql
                  SOURCES
                  driver.go
                  utils.h
                  utils.c
                  BUILD_TAGS
                  driverlib
                  PKG_CONFIG_NAME
                  adbc-driver-flightsql
                  SHARED_LINK_FLAGS
                  ${LDFLAGS})

include_directories(SYSTEM ${REPOSITORY_ROOT})
include_directories(SYSTEM ${REPOSITORY_ROOT}/c/)

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_flightsql_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_flightsql_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_dremio_flightsql_test
                PREFIX
                adbc
                SOURCES
                dremio_flightsql_test.cc
                ../../validation/adbc_validation.cc
                ../../validation/adbc_validation_util.cc
                EXTRA_LINK_LIBS
                nanoarrow
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-dremio-flightsql-test PRIVATE cxx_std_17)
  adbc_configure_target(adbc-driver-dremio-flightsql-test)
endif()

validate_config()
config_summary_message()