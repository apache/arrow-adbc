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

find_package(SQLite3)
if(SQLite3_FOUND)
  set(SQLite3_LINK_LIBRARIES SQLite::SQLite3)
else()
  # vcpkg
  find_package(unofficial-sqlite3 CONFIG REQUIRED)
  set(SQLite3_LINK_LIBRARIES unofficial::sqlite3::sqlite3)
  set(SQLite3_INCLUDE_DIRS)
endif()

# Check for sqlite3_load_extension() in sqlite3.h
if(EXISTS "${SQLite3_INCLUDE_DIRS}/sqlite3.h")
  file(READ "${SQLite3_INCLUDE_DIRS}/sqlite3.h" ADBC_SQLITE_H_CONTENT)
  string(FIND "${ADBC_SQLITE_H_CONTENT}" "sqlite3_load_extension"
              ADBC_SQLITE_WITH_LOAD_EXTENSION)
endif()

if(NOT ADBC_SQLITE_WITH_LOAD_EXTENSION)
  set(ADBC_SQLITE_COMPILE_DEFINES "-DADBC_SQLITE_WITH_NO_LOAD_EXTENSION")
endif()

add_arrow_lib(adbc_driver_sqlite
              SOURCES
              sqlite.cc
              statement_reader.c
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverSQLite
              PKG_CONFIG_NAME
              adbc-driver-sqlite
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${SQLite3_LINK_LIBRARIES}
              adbc_driver_common
              adbc_driver_framework
              STATIC_LINK_LIBS
              ${SQLite3_LINK_LIBRARIES}
              adbc_driver_common
              adbc_driver_framework
              ${LIBPQ_STATIC_LIBRARIES})

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING
                                                   ${ADBC_SQLITE_COMPILE_DEFINES})
  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${SQLite3_INCLUDE_DIRS} ${REPOSITORY_ROOT}/c/driver)

  if(NOT ADBC_DEFINE_COMMON_ENTRYPOINTS)
    target_compile_definitions(${LIB_TARGET} PRIVATE ${ADBC_TARGET_COMPILE_DEFINITIONS})
  endif()

  # On Windows, install sqlite3.dll alongside the driver
  if(WIN32 AND CMAKE_VERSION VERSION_GREATER_EQUAL "3.21")
    install(RUNTIME_DEPENDENCY_SET
            ${LIB_TARGET}_runtime_deps
            PRE_EXCLUDE_REGEXES
            "api-ms-"
            "ext-ms-"
            POST_EXCLUDE_REGEXES
            ".*system32/.*\\.dll"
            DESTINATION
            ${RUNTIME_INSTALL_DIR})
    install(TARGETS ${LIB_TARGET} RUNTIME_DEPENDENCY_SET ${LIB_TARGET}_runtime_deps)
  endif()
endforeach()

include(CheckTypeSize)
check_type_size("time_t" SIZEOF_TIME_T)
add_definitions(-DSIZEOF_TIME_T=${SIZEOF_TIME_T})

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_sqlite_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_sqlite_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_sqlite_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-sqlite
                SOURCES
                sqlite_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_definitions(adbc-driver-sqlite-test
                             PRIVATE ${ADBC_SQLITE_COMPILE_DEFINES})
  target_compile_features(adbc-driver-sqlite-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-sqlite-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS} ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-sqlite-test)
endif()
