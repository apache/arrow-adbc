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

project(adbc_driver_postgresql
        VERSION "${ADBC_BASE_VERSION}"
        LANGUAGES CXX)
include(CTest)

if(WIN32)
  # XXX: for now, assume vcpkg
  find_package(PostgreSQL REQUIRED)
  set(LIBPQ_LINK_LIBRARIES PostgreSQL::PostgreSQL wsock32 ws2_32)
  set(LIBPQ_STATIC_LIBRARIES PostgreSQL::PostgreSQL)
  set(LIBPQ_INCLUDE_DIRS)
else()
  find_package(PkgConfig)
  pkg_check_modules(LIBPQ REQUIRED libpq)
endif()

add_arrow_lib(adbc_driver_postgresql
              SOURCES
              connection.cc
              database.cc
              postgresql.cc
              statement.cc
              type.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverPostgreSQL
              PKG_CONFIG_NAME
              adbc-driver-postgresql
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${LIBPQ_LINK_LIBRARIES}
              nanoarrow
              STATIC_LINK_LIBS
              ${LIBPQ_LINK_LIBRARIES}
              nanoarrow
              ${LIBPQ_STATIC_LIBRARIES})
include_directories(SYSTEM ${REPOSITORY_ROOT})
include_directories(SYSTEM ${REPOSITORY_ROOT}/c/)
include_directories(SYSTEM ${LIBPQ_INCLUDE_DIRS})
foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
endforeach()

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_postgresql_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_postgresql_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_postgresql_test
                PREFIX
                adbc
                SOURCES
                postgres_copy_utils_test.cc
                postgres_type_test.cc
                postgresql_test.cc
                ../../validation/adbc_validation.cc
                ../../validation/adbc_validation_util.cc
                EXTRA_LINK_LIBS
                nanoarrow
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-postgresql-test PRIVATE cxx_std_17)
  adbc_configure_target(adbc-driver-postgresql-test)
endif()

validate_config()
config_summary_message()
