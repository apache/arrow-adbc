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

add_library(adbc_driver_framework STATIC objects.cc utility.cc)
adbc_configure_target(adbc_driver_framework)
set_target_properties(adbc_driver_framework PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(adbc_driver_framework
                           PRIVATE "${REPOSITORY_ROOT}/c/" "${REPOSITORY_ROOT}/c/include"
                                   "${REPOSITORY_ROOT}/c/vendor")
target_link_libraries(adbc_driver_framework PUBLIC adbc_driver_common fmt::fmt)

if(ADBC_BUILD_TESTS)
  add_test_case(driver_framework_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-framework
                SOURCES
                base_driver_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_framework)
  target_compile_features(adbc-driver-framework-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-framework-test
                             PRIVATE "${REPOSITORY_ROOT}/c/"
                                     "${REPOSITORY_ROOT}/c/include"
                                     "${REPOSITORY_ROOT}/c/vendor")
  adbc_configure_target(adbc-driver-framework-test)
endif()
