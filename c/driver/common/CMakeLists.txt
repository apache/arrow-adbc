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

add_library(adbc_driver_common STATIC utils.c)
target_compile_definitions(adbc_driver_common PRIVATE ADBC_STATIC)
adbc_configure_target(adbc_driver_common)
set_target_properties(adbc_driver_common PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_include_directories(adbc_driver_common PRIVATE "${REPOSITORY_ROOT}/c/include")
target_link_libraries(adbc_driver_common PUBLIC nanoarrow::nanoarrow)

# For static builds, we need to install the static library here so downstream
# applications can link to it
if(ADBC_BUILD_STATIC)
  if(ADBC_WITH_VENDORED_NANOARROW)
    message(WARNING "adbc_driver_common is not installed when ADBC_WITH_VENDORED_NANOARROW is ON. To use the static libraries, for now you must provide nanoarrow instead of using the vendored copy"
    )
  else()
    install(TARGETS adbc_driver_common ${INSTALL_IS_OPTIONAL}
            EXPORT adbc_driver_common_targets
            RUNTIME DESTINATION ${RUNTIME_INSTALL_DIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR})
    arrow_install_cmake_package(AdbcDriverCommon adbc_driver_common_targets)
  endif()
endif()

if(ADBC_BUILD_TESTS)
  add_test_case(driver_common_test
                PREFIX
                adbc
                EXTRA_LABELS
                driver-common
                SOURCES
                utils_test.cc
                EXTRA_LINK_LIBS
                adbc_driver_common)
  target_compile_features(adbc-driver-common-test PRIVATE cxx_std_17)
  target_include_directories(adbc-driver-common-test
                             PRIVATE "${REPOSITORY_ROOT}/c/include")
  adbc_configure_target(adbc-driver-common-test)
endif()
