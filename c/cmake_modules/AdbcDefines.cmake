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
# Must define REPOSITORY_ROOT before including this.

enable_language(C CXX)

if(${CMAKE_VERSION} VERSION_GREATER "3.24")
  cmake_policy(SET CMP0135 NEW)
endif()

set(BUILD_SUPPORT_DIR "${REPOSITORY_ROOT}/ci/build_support")

include(CheckLinkerFlag)
include(DefineOptions)
include(GNUInstallDirs) # Populates CMAKE_INSTALL_INCLUDEDIR
include(san-config)

# XXX: remove this, rely on user config
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

# ------------------------------------------------------------
# Common build utilities

# Link flags
set(ADBC_LINK_FLAGS)

set(ADBC_VERSION_SCRIPT_LINK_FLAG "-Wl,--version-script=${REPOSITORY_ROOT}/c/symbols.map")

check_linker_flag(CXX ${ADBC_VERSION_SCRIPT_LINK_FLAG} CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
if(CXX_LINKER_SUPPORTS_VERSION_SCRIPT)
  list(APPEND ADBC_LINK_FLAGS ${ADBC_VERSION_SCRIPT_LINK_FLAG})
endif()

# Set common build options
string(TOLOWER "${CMAKE_BUILD_TYPE}" _lower_build_type)
if("${ADBC_BUILD_WARNING_LEVEL}" STREQUAL "")
  if("${_lower_build_type}" STREQUAL "release")
    set(ADBC_BUILD_WARNING_LEVEL "PRODUCTION")
  else()
    set(ADBC_BUILD_WARNING_LEVEL "CHECKIN")
  endif()
endif()

if(MSVC)
  set(ADBC_C_CXX_FLAGS_CHECKIN /Wall /WX)
  set(ADBC_C_CXX_FLAGS_PRODUCTION /Wall)
  # Don't warn about strerror_s etc.
  add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
  # Allow incomplete switch (since MSVC warns even if there's a default case)
  add_compile_options(/wd4061)
  add_compile_options(/wd4100)
  add_compile_options(/wd4127)
  # Nanoarrow emits a lot of conversion warnings
  add_compile_options(/wd4365)
  add_compile_options(/wd4242)
  add_compile_options(/wd4458)
  add_compile_options(/wd4514)
  add_compile_options(/wd4582)
  add_compile_options(/wd4623)
  add_compile_options(/wd4625)
  add_compile_options(/wd4626)
  add_compile_options(/wd4868)
  add_compile_options(/wd4710)
  add_compile_options(/wd4711)
  # Don't warn about padding added after members
  add_compile_options(/wd4820)
  # Don't warn about enforcing left-to-right evaluation order for operator[]
  add_compile_options(/wd4866)
  add_compile_options(/wd5027)
  add_compile_options(/wd5039)
  add_compile_options(/wd5045)
  add_compile_options(/wd5246)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang"
       OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang"
       OR CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  # maybe-uninitialized is flaky
  set(ADBC_C_CXX_FLAGS_CHECKIN
      -Wall
      -Wextra
      -Wpedantic
      -Werror
      -Wno-unused-parameter)
  set(ADBC_C_CXX_FLAGS_PRODUCTION -Wall)

  if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    list(APPEND ADBC_C_CXX_FLAGS_CHECKIN -Wno-maybe-uninitialized)
  endif()

  if(NOT CMAKE_C_FLAGS_DEBUG MATCHES "-O")
    string(APPEND CMAKE_C_FLAGS_DEBUG " -Og")
  endif()
  if(NOT CMAKE_CXX_FLAGS_DEBUG MATCHES "-O")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " -Og")
  endif()

  if(NOT CMAKE_C_FLAGS_DEBUG MATCHES "-g")
    string(APPEND CMAKE_C_FLAGS_DEBUG " -g3")
  endif()
  if(NOT CMAKE_CXX_FLAGS_DEBUG MATCHES "-g")
    string(APPEND CMAKE_CXX_FLAGS_DEBUG " -g3")
  endif()
  if(NOT CMAKE_C_FLAGS_RELWITHDEBINFO MATCHES "-g")
    string(APPEND CMAKE_C_FLAGS_RELWITHDEBINFO " -g3")
  endif()
  if(NOT CMAKE_CXX_FLAGS_RELWITHDEBINFO MATCHES "-g")
    string(APPEND CMAKE_CXX_FLAGS_RELWITHDEBINFO " -g3")
  endif()
else()
  message(WARNING "Unknown compiler: ${CMAKE_CXX_COMPILER_ID}")
endif()

macro(adbc_configure_target TARGET)
  target_compile_options(${TARGET} PRIVATE ${ADBC_C_CXX_FLAGS_${ADBC_BUILD_WARNING_LEVEL}}
                                           ${ADBC_CXXFLAGS})
endmacro()

# Common testing setup
add_custom_target(all-tests)
if(ADBC_BUILD_TESTS)
  if(MSVC)
    # MSVC emitted warnings for testing code
    # Unary minus operator applied to unsigned type, result still unsigned
    add_compile_options(/wd4146)
    # An integer type is converted to a smaller integer type.
    add_compile_options(/wd4244)
    # Class has virtual functions, but its non-trivial destructor is not virtual; instances of this class may not be destructed correctly
    add_compile_options(/wd4265)
    # No override available for virtual member function from base
    add_compile_options(/wd4266)
    # Signed integral constant overflow
    add_compile_options(/wd4307)
    # Move constructor was implicitly defined as deleted
    add_compile_options(/wd5026)
    # Class has virtual functions, but its trivial destructor is not virtual
    add_compile_options(/wd5204)
    # Implicit fall-through occurs here
    add_compile_options(/wd5262)
  endif()

  find_package(GTest)
  if(NOT GTest_FOUND)
    message(STATUS "Building googletest from source")
    # Required for GoogleTest
    set(CMAKE_CXX_STANDARD 17)
    include(FetchContent)
    fetchcontent_declare(googletest
                         URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
    )
    # Windows: https://stackoverflow.com/questions/12540970/
    set(gtest_force_shared_crt
        ON
        CACHE BOOL "" FORCE)
    fetchcontent_makeavailable(googletest)
  endif()
  set(ADBC_TEST_LINK_LIBS GTest::gtest_main GTest::gtest GTest::gmock)
endif()
