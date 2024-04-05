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

include(FetchContent)
FetchContent_Declare(
  flatbuffers
  GIT_REPOSITORY https://github.com/google/flatbuffers.git
  GIT_TAG        v24.3.25
)
FetchContent_GetProperties(flatbuffers)
if(NOT flatbuffers_POPULATED)
  FetchContent_Populate(flatbuffers)
endif()
set(FLATBUFFERS_BUILD_TESTS OFF CACHE BOOL "")
set(FLATBUFFERS_INSTALL OFF CACHE BOOL "")
set(FLATBUFFERS_BUILD_FLATLIB OFF CACHE BOOL "")
add_subdirectory(${flatbuffers_SOURCE_DIR} ${flatbuffers_BINARY_DIR})

if(NOT EXISTS ${CMAKE_CURRENT_LIST_DIR}/include/arrow)
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/include/arrow)
endif()

if(NOT EXISTS ${CMAKE_CURRENT_LIST_DIR}/format)
  file(MAKE_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/format)
endif()

function(flatc_generate_header_files fbs_filenames fbs_sha256_checksums)
  set(FLATC_GENERATED_TARGETS "")
  foreach(fbs checksum IN ZIP_LISTS fbs_filenames fbs_sha256_checksums)
    set(LOCAL_FBS_FILENAME ${CMAKE_CURRENT_LIST_DIR}/format/${fbs}.fbs)
    if(NOT EXISTS "${LOCAL_FBS_FILENAME}")
      set(ARROW_PROTOCOL_FILE_BASE_URL "https://raw.githubusercontent.com/apache/arrow/apache-arrow-15.0.2/format")
      file(DOWNLOAD
        "${ARROW_PROTOCOL_FILE_BASE_URL}/${fbs}.fbs"
        "${LOCAL_FBS_FILENAME}"
        INACTIVITY_TIMEOUT 300
        SHOW_PROGRESS
        EXPECTED_HASH SHA256=${checksum}
      )
    endif()

    list(APPEND FLATC_GENERATED_TARGETS "flatc_generate_${fbs}")
    if(WIN32)
      set(FLATC_EXECUTABLE ${flatbuffers_BINARY_DIR}/${CMAKE_BUILD_TYPE}/flatc.exe)
    else()
      set(FLATC_EXECUTABLE ${flatbuffers_BINARY_DIR}/flatc)
    endif()
    add_custom_target(
      flatc_generate_${fbs} ${FLATC_EXECUTABLE} --no-warnings --cpp --cpp-std c++17 ${CMAKE_CURRENT_LIST_DIR}/format/${fbs}.fbs
      BYPRODUCTS ${CMAKE_CURRENT_LIST_DIR}/include/arrow/${fbs}_generated.h
      DEPENDS flatc ${CMAKE_CURRENT_LIST_DIR}/format/${fbs}.fbs
      WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR}/include/arrow
    )
  endforeach()
  set(FLATC_GENERATED_TARGETS ${FLATC_GENERATED_TARGETS} PARENT_SCOPE)
endfunction(flatc_generate_header_files)

set(FBS_FILES "")
set(FBS_FILE_SHA256_CHECKSUMS "")
# Message.fbs
list(APPEND FBS_FILES Message)
list(APPEND FBS_FILE_SHA256_CHECKSUMS "32a1464cede1b90d1351faf65726d465d95fbf3a85bd96e2d7222ac979fe20e4")
# Schema.fbs
list(APPEND FBS_FILES Schema)
list(APPEND FBS_FILE_SHA256_CHECKSUMS "34de85517cea535db69c5aeb7e486b331a31bb5a8acf1433099430feab2570df")
# SparseTensor.fbs
list(APPEND FBS_FILES SparseTensor)
list(APPEND FBS_FILE_SHA256_CHECKSUMS "4a340d4ed601fb1c3cb1d60c2bdb139bffa446c0f2c7d7e9fde5a28949075266")
# Tensor.fbs
list(APPEND FBS_FILES Tensor)
list(APPEND FBS_FILE_SHA256_CHECKSUMS "0ea5297a55f8ad00b125a98314f616c2c6249351bdbfea8f429913255b2a07f7")
flatc_generate_header_files("${FBS_FILES}" "${FBS_FILE_SHA256_CHECKSUMS}")

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  if(FLATC_GENERATED_TARGETS)
    add_dependencies(${LIB_TARGET} ${FLATC_GENERATED_TARGETS})
  endif()
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING "${ADBC_DRIVER_BIGQUERY_ENDIAN_DEF}")

  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}
                                     ${REPOSITORY_ROOT}/c/
                                     ${BIGQUERY_INCLUDE_DIRS}
                                     ${flatbuffers_SOURCE_DIR}/include
                                     ${CMAKE_CURRENT_LIST_DIR}/include
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
                                     ${flatbuffers_SOURCE_DIR}/include
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver)
  adbc_configure_target(adbc-driver-bigquery-test)
endif()
