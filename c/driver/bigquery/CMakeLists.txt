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
set(BIGQUERY_HASH_X86_64_APPLE_DARWIN "e67daf06bab384707a9f4189234e870870e2fd305ee00abfbbbb71b815868282")
set(BIGQUERY_HASH_AARCH64_APPLE_DARWIN "cdbcbfc0783524e95f772b98c20e001249ce495935a39f3d11a90b314f6f92f5")
set(BIGQUERY_HASH_X86_64_LINUX_GNU "917c85415be560ed02f3a8d359790865d8a8625a14dd036a1baf1f99aaeb90db")
set(BIGQUERY_HASH_AARCH64_LINUX_GNU "68f29e25ca7f1fe1ae891eba08b9c9c051662cd1808adc89c55bbd4c65c051ca")
set(BIGQUERY_HASH_X86_64_WINDOWS_MSVC "fb24d944ef458036e494fd9970905e7dbcbea18a5cbcac727fb875b4dc6c3f39")
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
find_package(google_cloud_cpp_bigquery REQUIRED PATHS "${BIGQUERY_INSTALL_PREFIX}")
set(BIGQUERY_LINK_LIBRARIES google-cloud-cpp::bigquery)

add_arrow_lib(adbc_driver_bigquery
              SOURCES
              bigquery.cc
              connection.cc
              database.cc
              statement.cc
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
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
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
                                     ${LIBPQ_INCLUDE_DIRS}
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-bigquery-test)
endif()
