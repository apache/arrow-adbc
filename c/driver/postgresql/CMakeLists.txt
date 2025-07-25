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

# The Redshift driver requires the AWS SDK
if(ADBC_REDSHIFT_FLAVOR)
  find_package(ZLIB REQUIRED)
  find_package(AWSSDK CONFIG
               COMPONENTS core sts redshift
               REQUIRED)
endif()

# Set library name based on flavor
if(ADBC_REDSHIFT_FLAVOR)
  set(DRIVER_LIB_NAME adbc_driver_redshift)
else()
  set(DRIVER_LIB_NAME adbc_driver_postgresql)
endif()

add_arrow_lib(${DRIVER_LIB_NAME}
              SOURCES
              connection.cc
              error.cc
              database.cc
              redshift_auth.cc
              postgresql.cc
              result_helper.cc
              result_reader.cc
              statement.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverPostgreSQL
              PKG_CONFIG_NAME
              adbc-driver-postgresql
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
              adbc_driver_common
              adbc_driver_framework
              ${LIBPQ_LINK_LIBRARIES}
              STATIC_LINK_LIBS
              ${LIBPQ_LINK_LIBRARIES}
              adbc_driver_common
              adbc_driver_framework
              ${LIBPQ_STATIC_LIBRARIES})

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS} ${REPOSITORY_ROOT}/c/driver)

  if(ADBC_REDSHIFT_FLAVOR)
    target_compile_definitions(${LIB_TARGET} PUBLIC ADBC_REDSHIFT_FLAVOR)
    target_include_directories(${LIB_TARGET} SYSTEM PRIVATE ${AWSSDK_INCLUDE_DIRS})
    target_link_libraries(${LIB_TARGET} PRIVATE ${AWSSDK_LIBRARIES})
  endif()
endforeach()

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS ${DRIVER_LIB_NAME}_shared)
else()
  set(TEST_LINK_LIBS ${DRIVER_LIB_NAME}_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_postgresql_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-postgresql
                SOURCES
                postgres_type_test.cc
                postgresql_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-postgresql-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-postgresql-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS} ${REPOSITORY_ROOT}/c/driver)
  if(ADBC_REDSHIFT_FLAVOR)
    target_include_directories(adbc-driver-postgresql-test SYSTEM
                               PRIVATE ${AWSSDK_INCLUDE_DIRS})
    target_link_libraries(adbc-driver-postgresql-test PRIVATE ${AWSSDK_LIBRARIES})
  endif()
  adbc_configure_target(adbc-driver-postgresql-test)

  add_test_case(driver_postgresql_copy_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-postgresql
                SOURCES
                copy/postgres_copy_reader_test.cc
                copy/postgres_copy_writer_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-postgresql-copy-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-postgresql-copy-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS} ${REPOSITORY_ROOT}/c/driver)
  if(ADBC_REDSHIFT_FLAVOR)
    target_include_directories(adbc-driver-postgresql-copy-test SYSTEM
                               PRIVATE ${AWSSDK_INCLUDE_DIRS})
    target_link_libraries(adbc-driver-postgresql-copy-test PRIVATE ${AWSSDK_LIBRARIES})
  endif()
  adbc_configure_target(adbc-driver-postgresql-copy-test)
endif()

if(ADBC_BUILD_BENCHMARKS)
  find_package(benchmark REQUIRED)
  # TODO: should add_benchmark be linking benchmark::benchmark for us?
  add_benchmark(postgresql_benchmark
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS}
                benchmark::benchmark)
  # add_benchmark replaces _ with - when creating target
  target_include_directories(postgresql-benchmark
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${REPOSITORY_ROOT}/c/driver)
endif()
