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
FetchContent_Declare(
  tomlplusplus
  GIT_REPOSITORY https://github.com/marzer/tomlplusplus.git
  GIT_TAG v3.4.0
)
FetchContent_MakeAvailable(tomlplusplus)

add_arrow_lib(adbc_driver_manager
              SOURCES
              adbc_driver_manager.cc
              OUTPUTS
              ADBC_LIBRARIES
              CMAKE_PACKAGE_NAME
              AdbcDriverManager
              PKG_CONFIG_NAME
              adbc-driver-manager
              SHARED_LINK_LIBS
              ${CMAKE_DL_LIBS}
              STATIC_LINK_LIBS
              tomlplusplus::tomlplusplus
              ${CMAKE_DL_LIBS}
              SHARED_LINK_FLAGS
              ${ADBC_LINK_FLAGS})

install(FILES "${REPOSITORY_ROOT}/c/include/adbc.h" DESTINATION include)
install(FILES "${REPOSITORY_ROOT}/c/include/arrow-adbc/adbc.h"
        DESTINATION include/arrow-adbc)

foreach(LIB_TARGET ${ADBC_LIBRARIES})
  target_include_directories(${LIB_TARGET} SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${REPOSITORY_ROOT}/c/vendor
                                     ${REPOSITORY_ROOT}/c/driver
                                     ${tomlplusplus_SOURCE_DIR}/include)
  target_compile_definitions(${LIB_TARGET} PRIVATE ADBC_EXPORTING)
endforeach()

if(ADBC_BUILD_TESTS)
  if(ADBC_TEST_LINKAGE STREQUAL "shared")
    set(TEST_LINK_LIBS adbc_driver_manager_shared)
  else()
    set(TEST_LINK_LIBS adbc_driver_manager_static)
  endif()

  add_test_case(driver_manager_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-manager
                SOURCES
                adbc_driver_manager_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common
                adbc_validation
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-driver-manager-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-manager-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS} ${REPOSITORY_ROOT}/c/driver)

  add_test_case(version_100_compatibility_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-manager
                SOURCES
                adbc_version_100.c
                adbc_version_100_compatibility_test.cc
                EXTRA_LINK_LIBS
                adbc_validation_util
                ${TEST_LINK_LIBS})
  target_compile_features(adbc-version-100-compatibility-test PRIVATE cxx_std_17)
  target_include_directories(adbc-version-100-compatibility-test SYSTEM
                             PRIVATE ${REPOSITORY_ROOT}/c/ ${REPOSITORY_ROOT}/c/include/
                                     ${LIBPQ_INCLUDE_DIRS} ${REPOSITORY_ROOT}/c/driver)
endif()
