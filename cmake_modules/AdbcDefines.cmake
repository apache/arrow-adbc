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

# Common definitions for the CMake projects in this repository.

enable_language(C)
include(DefineOptions)

set(ARROW_VERSION "9.0.0-SNAPSHOT")
set(ARROW_BASE_VERSION "9.0.0")
set(ARROW_VERSION_MAJOR "9")
set(ARROW_VERSION_MINOR "0")
set(ARROW_VERSION_PATCH "0")

math(EXPR ARROW_SO_VERSION "${ARROW_VERSION_MAJOR} * 100 + ${ARROW_VERSION_MINOR}")
set(ARROW_FULL_SO_VERSION "${ARROW_SO_VERSION}.${ARROW_VERSION_PATCH}.0")

if(ARROW_DEPENDENCY_SOURCE STREQUAL "CONDA")
  message(STATUS "Adding \$CONDA_PREFIX to CMAKE_PREFIX_PATH")
  list(APPEND CMAKE_PREFIX_PATH "$ENV{CONDA_PREFIX}")
endif()

if(ARROW_BUILD_TESTS)
  add_custom_target(all-tests)
  find_package(GTest)
  set(ARROW_TEST_LINK_LIBS GTest::gtest_main GTest::gtest GTest::gmock)
endif()
