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

cmake_minimum_required(VERSION 3.10)

set(ARROW_VERSION "9.0.0-SNAPSHOT")
set(ARROW_BASE_VERSION "9.0.0")
set(ARROW_VERSION_MAJOR "9")
set(ARROW_VERSION_MINOR "0")
set(ARROW_VERSION_PATCH "0")

math(EXPR ARROW_SO_VERSION "${ARROW_VERSION_MAJOR} * 100 + ${ARROW_VERSION_MINOR}")
set(ARROW_FULL_SO_VERSION "${ARROW_SO_VERSION}.${ARROW_VERSION_PATCH}.0")

project(adbc_driver_manager VERSION "${ARROW_BASE_VERSION}" LANGUAGES CXX)

# Common CMake utilities
get_filename_component(REPOSITORY_ROOT ${CMAKE_SOURCE_DIR} DIRECTORY)
list(APPEND CMAKE_MODULE_PATH "${REPOSITORY_ROOT}/cmake_modules/")
include(BuildUtils)
include(DefineOptions)

# Driver manager library
add_arrow_lib(adbc_driver_manager
  SOURCES
  adbc_driver_manager.cc)
include_directories(adbc_driver_manager SYSTEM
  PRIVATE ${REPOSITORY_ROOT})

# Tests

validate_config()
config_summary_message()
