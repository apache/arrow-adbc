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

project(adbc_cookbook_recipes_driver
        VERSION "1.0.0"
        LANGUAGES CXX)

include(CTest)
include(FetchContent)

set(CMAKE_CXX_STANDARD 17)

set(NANOARROW_IPC ON)
set(NANOARROW_NAMESPACE "DriverExamplePrivate")
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

fetchcontent_declare(fmt
                     GIT_REPOSITORY https://github.com/fmtlib/fmt
                     GIT_TAG 1be298e1bd68957e4cd352e1f676f00e07dcfb57) # 12.2.0
fetchcontent_makeavailable(fmt)

fetchcontent_declare(nanoarrow
                     GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
                     GIT_TAG apache-arrow-nanoarrow-0.9.0
                     GIT_SHALLOW TRUE
                     EXCLUDE_FROM_ALL)
fetchcontent_makeavailable(nanoarrow)

# TODO: We could allow this to be installed + linked to as a target; however,
# fetchcontent is a little nicer for this kind of thing (statically linked
# pinned version of something that doesn't rely on a system library).
add_library(adbc_driver_framework ../../../../c/driver/framework/utility.cc
                                  ../../../../c/driver/framework/objects.cc)
target_include_directories(adbc_driver_framework PRIVATE ../../../../c
                                                         ../../../../c/include)
target_link_libraries(adbc_driver_framework PRIVATE fmt::fmt nanoarrow::nanoarrow_static)

add_library(driver_example SHARED driver_example.cc)
target_include_directories(driver_example PRIVATE ../../../../c ../../../../c/include)
target_link_libraries(driver_example PRIVATE adbc_driver_framework fmt::fmt
                                             nanoarrow::nanoarrow_ipc)

install(TARGETS driver_example)

include(./get_arch.cmake)
cmake_path(SET DRIVER_PATH "${CMAKE_INSTALL_PREFIX}")
cmake_path(APPEND DRIVER_PATH "${CMAKE_INSTALL_LIBDIR}"
           "${CMAKE_SHARED_LIBRARY_PREFIX}driver_example${CMAKE_SHARED_LIBRARY_SUFFIX}")

configure_file(driver_example.toml.in "${CMAKE_CURRENT_SOURCE_DIR}/driver_example.toml"
               @ONLY)

if(ADBC_DRIVER_EXAMPLE_BUILD_TESTS)
  fetchcontent_declare(googletest
                       URL https://github.com/google/googletest/archive/refs/tags/v1.15.1.tar.gz
                       URL_HASH SHA256=5052e088b16bdd8c6f0c7f9cafc942fd4f7c174f1dac6b15a8dd83940ed35195
  )
  fetchcontent_makeavailable(googletest)

  find_package(AdbcDriverManager REQUIRED)

  add_executable(driver_example_test driver_example_test.cc)
  target_link_libraries(driver_example_test
                        PRIVATE gtest_main driver_example
                                AdbcDriverManager::adbc_driver_manager_shared)

  include(GoogleTest)
  gtest_discover_tests(driver_example_test)

endif()
