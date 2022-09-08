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
get_filename_component(REPOSITORY_ROOT "../../../" ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${REPOSITORY_ROOT}/c/cmake_modules/")
include(AdbcDefines)
include(BuildUtils)

project(adbc_driver_postgres
        VERSION "${ADBC_BASE_VERSION}"
        LANGUAGES CXX)
include(CTest)
find_package(PkgConfig)

pkg_check_modules(LIBPQ REQUIRED libpq)

add_arrow_lib(adbc_driver_postgres
              SOURCES
              connection.cc
              database.cc
              postgres.cc
              statement.cc
              type.cc
              ${REPOSITORY_ROOT}/c/vendor/nanoarrow/nanoarrow.c
              OUTPUTS
              ADBC_LIBRARIES
              SHARED_LINK_LIBS
              ${LIBPQ_LINK_LIBRARIES}
              STATIC_LINK_LIBS
              ${LIBPQ_STATIC_LIBRARIES})
include_directories(SYSTEM ${REPOSITORY_ROOT})
include_directories(SYSTEM ${REPOSITORY_ROOT}/c/)
include_directories(SYSTEM ${REPOSITORY_ROOT}/c/vendor/nanoarrow/)
include_directories(SYSTEM ${LIBPQ_INCLUDE_DIRS})
foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
endforeach()

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_postgres_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_postgres_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_postgres_test
                PREFIX
                adbc
                SOURCES
                postgres_test.cc
                ../../validation/adbc_validation.cc
                ${REPOSITORY_ROOT}/c/vendor/nanoarrow/nanoarrow.c
                EXTRA_LINK_LIBS
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-postgres-test PRIVATE cxx_std_17)
endif()

validate_config()
config_summary_message()
