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

cmake_minimum_required(VERSION 3.16)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

# find_package() uses <PackageName>_ROOT variables.
# https://cmake.org/cmake/help/latest/policy/CMP0074.html
if(POLICY CMP0074)
  cmake_policy(SET CMP0074 NEW)
endif()

project(adbc-java)

if("${CMAKE_CXX_STANDARD}" STREQUAL "")
  set(CMAKE_CXX_STANDARD 17)
endif()
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)

# Dependencies

find_package(Java REQUIRED)
find_package(JNI REQUIRED)

include(UseJava)

# ADBC_ARCH_DIR is derived from the architecture. The user can override this
# variable if auto-detection fails.
if("${ADBC_ARCH_DIR}" STREQUAL "")
  if("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "aarch64")
    set(ADBC_ARCH_DIR "aarch_64")
  elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "i386")
    set(ADBC_ARCH_DIR "x86_64")
  elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "arm64")
    set(ADBC_ARCH_DIR "aarch_64")
  elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "AMD64")
    set(ADBC_ARCH_DIR "x86_64")
  elseif("${CMAKE_SYSTEM_PROCESSOR}" STREQUAL "x86_64")
    set(ADBC_ARCH_DIR "x86_64")
  else()
    message(FATAL_ERROR "Unsupported architecture: ${CMAKE_SYSTEM_PROCESSOR}")
  endif()
endif()

add_subdirectory(driver/jni)
