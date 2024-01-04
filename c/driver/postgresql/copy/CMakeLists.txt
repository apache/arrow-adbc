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

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_postgresql_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_postgresql_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_postgresql_copy_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-postgresql
                SOURCES
                postgres_copy_reader_test.cc
                postgres_copy_writer_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                nanoarrow
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-postgresql-copy-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-postgresql-copy-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-postgresql-copy-test)
endif()
