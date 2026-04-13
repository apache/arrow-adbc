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

# --- Locate DB2 CLI / ODBC headers and library ---

set(DB2_HOME_HINTS "")
if(DEFINED ENV{DB2_HOME})
  list(APPEND DB2_HOME_HINTS "$ENV{DB2_HOME}")
endif()
if(DEFINED ENV{IBM_DB_HOME})
  list(APPEND DB2_HOME_HINTS "$ENV{IBM_DB_HOME}")
endif()

find_path(DB2_INCLUDE_DIR
          NAMES sqlcli1.h sql.h
          HINTS ${DB2_HOME_HINTS}
          PATH_SUFFIXES include
          DOC "IBM DB2 CLI header directory")

if(WIN32)
  set(_db2_lib_names db2cli db2cli64)
else()
  set(_db2_lib_names db2)
endif()

find_library(DB2_LIBRARY
             NAMES ${_db2_lib_names}
             HINTS ${DB2_HOME_HINTS}
             PATH_SUFFIXES lib lib64
             DOC "IBM DB2 CLI library")

if(NOT DB2_INCLUDE_DIR)
  message(WARNING "DB2 CLI headers not found. "
                  "Set DB2_HOME or IBM_DB_HOME, or install unixODBC headers as fallback. "
                  "Building with system ODBC headers.")
  find_path(DB2_INCLUDE_DIR
            NAMES sql.h sqlext.h
            DOC "ODBC header directory (fallback for DB2 CLI)")
endif()

if(NOT DB2_LIBRARY)
  message(WARNING "DB2 CLI library not found. "
                  "Set DB2_HOME or IBM_DB_HOME. "
                  "Attempting to link with system ODBC library as fallback.")
  if(WIN32)
    find_library(DB2_LIBRARY NAMES odbc32 DOC "ODBC library (fallback)")
  elseif(APPLE)
    find_library(DB2_LIBRARY NAMES iodbc odbc DOC "ODBC library (fallback)")
  else()
    find_library(DB2_LIBRARY NAMES odbc DOC "ODBC library (fallback)")
  endif()
endif()

if(NOT DB2_INCLUDE_DIR OR NOT DB2_LIBRARY)
  message(FATAL_ERROR "Could not find DB2 CLI or ODBC headers/library. "
                      "Please set DB2_HOME or install unixODBC-dev / iODBC.")
endif()

message(STATUS "DB2 CLI include: ${DB2_INCLUDE_DIR}")
message(STATUS "DB2 CLI library: ${DB2_LIBRARY}")

add_arrow_lib(adbc_driver_db2
              SOURCES
              db2.cc
              database.cc
              connection.cc
              statement.cc
              result_reader.cc
              type_mapping.cc
              error.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverDb2
              PKG_CONFIG_NAME
              adbc-driver-db2
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${DB2_LIBRARY}
              adbc_driver_common
              adbc_driver_framework
              STATIC_LINK_LIBS
              ${DB2_LIBRARY}
              adbc_driver_common
              adbc_driver_framework)

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${DB2_INCLUDE_DIR}
                                     ${REPOSITORY_ROOT}/c/driver)

  if(NOT ADBC_DEFINE_COMMON_ENTRYPOINTS)
    target_compile_definitions(${LIB_TARGET} PRIVATE ${ADBC_TARGET_COMPILE_DEFINITIONS})
  endif()
endforeach()

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_db2_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_db2_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_db2_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-db2
                SOURCES
                db2_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-db2-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-db2-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/
                                     ${REPOSITORY_ROOT}/c/include/
                                     ${DB2_INCLUDE_DIR}
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-db2-test)
endif()
