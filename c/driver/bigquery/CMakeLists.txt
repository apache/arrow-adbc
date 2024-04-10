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

if(NOT DEFINED BIGQUERY_INSTALL_PREFIX OR "${BIGQUERY_INSTALL_PREFIX}" STREQUAL "")
  set(BIGQUERY_INSTALL_PREFIX "${CMAKE_CURRENT_SOURCE_DIR}/install")
endif()

if(NOT DEFINED CMAKE_CXX_STANDARD)
  set(CMAKE_CXX_STANDARD 17)
endif()

# == Generated checksum ==
set(BIGQUERY_VERSION_WITH_HASH "2.22.0")
set(BIGQUERY_HASH_X86_64_APPLE_DARWIN "a091610c74012c2787c89a8512f216dfc916e3650d9597e0026ede5d39121a66")
set(BIGQUERY_HASH_AARCH64_APPLE_DARWIN "cf130f7da54258b493645e8d39f98e92ee0d9508b8e16d9c779277fe42c96ea9")
set(BIGQUERY_HASH_X86_64_LINUX_GNU "9f556f03f3855d0f4aebccf2f1a3784d876c2e94fa295975ddc450ae7492affc")
set(BIGQUERY_HASH_AARCH64_LINUX_GNU "9fd28d96b55843a8c5cce5ba69795d387c068092fd802dd0082bb0f97e8c4ed7")
set(BIGQUERY_HASH_X86_64_WINDOWS_MSVC "757a3b1d9b417d312f4812fd5104c3f6b40db6b41921265e2811ee47234bacc9")
# == Generated checksum ==

if(NOT DEFINED BIGQUERY_VERSION OR "${BIGQUERY_VERSION}" STREQUAL "")
  set(BIGQUERY_VERSION "${BIGQUERY_VERSION_WITH_HASH}")
endif()
set(PRECOMPILED_BIGQUERY_BASE_URL "https://github.com/cocoa-xu/google-cloud-cpp-bigquery/releases/download/v${BIGQUERY_VERSION}")

if(UNIX)
  if(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-x86_64-apple-darwin.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_X86_64_APPLE_DARWIN}")
      endif()
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "arm64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-aarch64-apple-darwin.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_AARCH64_APPLE_DARWIN}")
      endif()
    else()
      message(FATAL_ERROR "Unsupported CPU architecture: ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
  else()
    if(CMAKE_SYSTEM_PROCESSOR STREQUAL "x86_64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-x86_64-linux-gnu.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_X86_64_LINUX_GNU}")
      endif()
    elseif(CMAKE_SYSTEM_PROCESSOR STREQUAL "aarch64")
      set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-aarch64-linux-gnu.tar.gz")
      if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
        set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_AARCH64_LINUX_GNU}")
      endif()
    else()
      message(FATAL_ERROR "Unsupported CPU architecture: ${CMAKE_SYSTEM_PROCESSOR}")
    endif()
  endif()
elseif(WIN32)
  if(CMAKE_SYSTEM_PROCESSOR STREQUAL "AMD64")
    set(PRECOMPILED_BIGQUERY_TARBALL_FILENAME "bigquery-${BIGQUERY_VERSION}-x86_64-windows-msvc.tar.gz")
    if(BIGQUERY_VERSION STREQUAL BIGQUERY_VERSION_WITH_HASH)
      set(PRECOMPILED_BIGQUERY_HASH "${BIGQUERY_HASH_X86_64_WINDOWS_MSVC}")
    endif()
  else()
    message(FATAL_ERROR "Unsupported CPU architecture: ${CMAKE_SYSTEM_PROCESSOR}")
  endif()
else()
  message(FATAL_ERROR "Unsupported operating system: ${CMAKE_SYSTEM_NAME}")
endif()
set(PRECOMPILED_BIGQUERY_URL "${PRECOMPILED_BIGQUERY_BASE_URL}/${PRECOMPILED_BIGQUERY_TARBALL_FILENAME}")

set(CACHE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/cache")
set(PRECOMPILED_BIGQUERY_TARBALL "${CACHE_DIR}/${PRECOMPILED_BIGQUERY_TARBALL_FILENAME}")
if(UNIX)
  set(PRECOMPILED_BIGQUERY_ARCHIVE "${BIGQUERY_INSTALL_PREFIX}/lib/libgoogle_cloud_cpp_bigquery.a")
elseif(WIN32)
  set(PRECOMPILED_BIGQUERY_ARCHIVE "${BIGQUERY_INSTALL_PREFIX}/lib/google_cloud_cpp_bigquery.lib")
else()
  message(FATAL_ERROR "Unsupported operating system: ${CMAKE_SYSTEM_NAME}")
endif()

if(NOT EXISTS "${PRECOMPILED_BIGQUERY_ARCHIVE}")
  if(NOT EXISTS "${PRECOMPILED_BIGQUERY_TARBALL}")
    message(STATUS "Using precompiled bigquery binaries from ${PRECOMPILED_BIGQUERY_URL}")
    if(DEFINED PRECOMPILED_BIGQUERY_HASH)
      file(DOWNLOAD
        "${PRECOMPILED_BIGQUERY_URL}"
        "${PRECOMPILED_BIGQUERY_TARBALL}"
        INACTIVITY_TIMEOUT 300
        SHOW_PROGRESS
        EXPECTED_HASH SHA256=${PRECOMPILED_BIGQUERY_HASH}
      )
    else()
      file(DOWNLOAD
        "${PRECOMPILED_BIGQUERY_URL}"
        "${PRECOMPILED_BIGQUERY_TARBALL}"
        INACTIVITY_TIMEOUT 300
        SHOW_PROGRESS
      )
    endif()
  endif()

  if(UNIX)
    execute_process(
      COMMAND bash -c "mkdir -p \"${BIGQUERY_INSTALL_PREFIX}\" && tar xzf \"${PRECOMPILED_BIGQUERY_TARBALL}\" -C \"${BIGQUERY_INSTALL_PREFIX}\""
      RESULT_VARIABLE STATUS
    )
  elseif(WIN32)
    execute_process(
      COMMAND powershell -command "(New-Item -ItemType Directory -Path \"${BIGQUERY_INSTALL_PREFIX}\" -Force -ErrorAction SilentlyContinue) -and (tar xzf \"${PRECOMPILED_BIGQUERY_TARBALL}\" -C \"${BIGQUERY_INSTALL_PREFIX}\")"
      RESULT_VARIABLE STATUS
    )
  else()
    message(FATAL_ERROR "Unsupported operating system: ${CMAKE_SYSTEM_NAME}")
  endif()
  if(STATUS STREQUAL "0")
    message(STATUS "Precompiled bigquery binaries extracted to ${BIGQUERY_INSTALL_PREFIX}")
  else()
    message(FATAL_ERROR "Failed to extract bigquery binaries from ${PRECOMPILED_BIGQUERY_TARBALL} to ${BIGQUERY_INSTALL_PREFIX}")
  endif()
endif()

set(CMAKE_PREFIX_PATH "${BIGQUERY_INSTALL_PREFIX};${BIGQUERY_INSTALL_PREFIX}/openssl;${CMAKE_PREFIX_PATH}")
set(BIGQUERY_INCLUDE_DIRS "${BIGQUERY_INSTALL_PREFIX}/include")

find_package(OpenSSL REQUIRED)
find_package(Arrow REQUIRED)
find_package(google_cloud_cpp_bigquery REQUIRED PATHS "${BIGQUERY_INSTALL_PREFIX}")
find_package(google_cloud_cpp_bigquery_rest REQUIRED PATHS "${BIGQUERY_INSTALL_PREFIX}")

set(BIGQUERY_LINK_LIBRARIES google-cloud-cpp::bigquery google-cloud-cpp::experimental-bigquery_rest Arrow::arrow_static)

if(NOT DEFINED ADBC_DRIVER_BIGQUERY_ENDIAN)
  if(CMAKE_VERSION VERSION_LESS "3.20.0")
    INCLUDE(TestBigEndian)
    TEST_BIG_ENDIAN(ADBC_DRIVER_BIGQUERY_ENDIAN_TEST)
    if(ADBC_DRIVER_BIGQUERY_ENDIAN_TEST)
      set(ADBC_DRIVER_BIGQUERY_ENDIAN BIG_ENDIAN)
    else()
      set(ADBC_DRIVER_BIGQUERY_ENDIAN LITTLE_ENDIAN)
    endif()
  else()
    if(DEFINED CMAKE_CXX_BYTE_ORDER AND CMAKE_CXX_BYTE_ORDER STREQUAL "BIG_ENDIAN")
      set(ADBC_DRIVER_BIGQUERY_ENDIAN BIG_ENDIAN)
    else()
      set(ADBC_DRIVER_BIGQUERY_ENDIAN LITTLE_ENDIAN)
    endif()
  endif()
endif()
message(STATUS "ADBC_DRIVER_BIGQUERY_ENDIAN: ${ADBC_DRIVER_BIGQUERY_ENDIAN}")
if(ADBC_DRIVER_BIGQUERY_ENDIAN STREQUAL "BIG_ENDIAN")
  set(ADBC_DRIVER_BIGQUERY_ENDIAN_DEF "ADBC_DRIVER_BIGQUERY_ENDIAN=0")
else()
  set(ADBC_DRIVER_BIGQUERY_ENDIAN_DEF "ADBC_DRIVER_BIGQUERY_ENDIAN=1")
endif()

add_arrow_lib(adbc_driver_bigquery
              SOURCES
              bigquery.cc
              connection.cc
              database.cc
              statement.cc
              readrowsiterator.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverBigQuery
              PKG_CONFIG_NAME
              adbc-driver-bigquery
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS}
              SHARED_LINK_LIBS
              ${BIGQUERY_LINK_LIBRARIES}
              adbc_driver_common
              nanoarrow
              STATIC_LINK_LIBS
              ${BIGQUERY_LINK_LIBRARIES}
              adbc_driver_common
              nanoarrow
              ${BIGQUERY_LINK_LIBRARIES})

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING "${ADBC_DRIVER_BIGQUERY_ENDIAN_DEF}")

  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${BIGQUERY_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
endforeach()

if(ADBC_TEST_LINKAGE STREQUAL "shared")
  set(TEST_LINK_LIBS adbc_driver_bigquery_shared)
else()
  set(TEST_LINK_LIBS adbc_driver_bigquery_static)
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_bigquery_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-bigquery
                SOURCES
                bigquery_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                nanoarrow
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-bigquery-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-bigquery-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${BIGQUERY_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-bigquery-test)
endif()
