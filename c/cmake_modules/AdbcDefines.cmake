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

enable_language(C CXX)

set(BUILD_SUPPORT_DIR "${REPOSITORY_ROOT}/ci/build_support")

include(DefineOptions)
include(GNUInstallDirs) # Populates CMAKE_INSTALL_INCLUDEDIR
include(san-config)

set(ADBC_VERSION "1.0.0-SNAPSHOT")
set(ADBC_BASE_VERSION "1.0.0")
set(ADBC_VERSION_MAJOR "1")
set(ADBC_VERSION_MINOR "0")
set(ADBC_VERSION_PATCH "0")

math(EXPR ADBC_SO_VERSION "${ADBC_VERSION_MAJOR} * 100 + ${ADBC_VERSION_MINOR}")
set(ADBC_FULL_SO_VERSION "${ADBC_SO_VERSION}.${ADBC_VERSION_PATCH}.0")

if(ADBC_DEPENDENCY_SOURCE STREQUAL "CONDA")
  message(STATUS "Adding \$CONDA_PREFIX to CMAKE_PREFIX_PATH")
  list(APPEND CMAKE_PREFIX_PATH "$ENV{CONDA_PREFIX}")
endif()

# pkg-config (.pc file) support.
if(IS_ABSOLUTE "${CMAKE_INSTALL_INCLUDEDIR}")
  set(ADBC_PKG_CONFIG_INCLUDEDIR "${CMAKE_INSTALL_INCLUDEDIR}")
else()
  set(ADBC_PKG_CONFIG_INCLUDEDIR "\${prefix}/${CMAKE_INSTALL_INCLUDEDIR}")
endif()
if(IS_ABSOLUTE "${CMAKE_INSTALL_LIBDIR}")
  set(ADBC_PKG_CONFIG_LIBDIR "${CMAKE_INSTALL_LIBDIR}")
else()
  set(ADBC_PKG_CONFIG_LIBDIR "\${prefix}/${CMAKE_INSTALL_LIBDIR}")
endif()

add_custom_target(all-tests)
if(ADBC_BUILD_TESTS)
  find_package(GTest)
  set(ADBC_TEST_LINK_LIBS GTest::gtest_main GTest::gtest GTest::gmock)
endif()
