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

include(FetchContent)

if(ADBC_BUILD_TESTS)
  fetchcontent_declare(duckdb
                       GIT_REPOSITORY https://github.com/duckdb/duckdb.git
                       GIT_TAG 8e52ec43959ab363643d63cb78ee214577111da4 # v1.2.1
                       GIT_PROGRESS TRUE
                       USES_TERMINAL_DOWNLOAD TRUE)
  set(BUILD_JEMALLOC_EXTENSION
      OFF
      CACHE INTERNAL "Disable")
  set(BUILD_NODE
      OFF
      CACHE INTERNAL "Disable")
  set(BUILD_PYTHON
      OFF
      CACHE INTERNAL "Disable")
  set(BUILD_R
      OFF
      CACHE INTERNAL "Disable")
  set(BUILD_SHELL
      OFF
      CACHE INTERNAL "Disable")
  set(BUILD_UNITTESTS
      OFF
      CACHE INTERNAL "Disable")
  set(ENABLE_SANITIZER
      OFF
      CACHE INTERNAL "Disable ASAN")
  set(ENABLE_UBSAN
      OFF
      CACHE INTERNAL "Disable UBSAN")
  # Force cmake to honor our options here in the subproject
  cmake_policy(SET CMP0077 NEW)
  message(STATUS "Fetching DuckDB")
  fetchcontent_makeavailable(duckdb)

  add_test_case(integration_duckdb_test
                PREFIX
                adbc
                EXTRA_LABELS
                integration-duckdb
                SOURCES
                duckdb_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_driver_manager_static
                adbc_validation
                duckdb)
  add_dependencies(adbc-integration-duckdb-test duckdb)
  target_compile_features(adbc-integration-duckdb-test PRIVATE cxx_std_17)
  target_include_directories(adbc-integration-duckdb-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-integration-duckdb-test)
endif()
