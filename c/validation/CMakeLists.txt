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
get_filename_component(REPOSITORY_ROOT "../../" ABSOLUTE)
list(APPEND CMAKE_MODULE_PATH "${REPOSITORY_ROOT}/c/cmake_modules/")
include(AdbcVersion)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
project(adbc_validation
        VERSION "${ADBC_BASE_VERSION}"
        LANGUAGES CXX)

find_package(GTest REQUIRED)
get_filename_component(REPOSITORY_ROOT "../../" ABSOLUTE)

add_library(adbc_validation STATIC adbc_validation.cc adbc_validation_util.cc)
target_compile_features(adbc_validation PRIVATE cxx_std_17)
target_include_directories(adbc_validation SYSTEM PRIVATE "${REPOSITORY_ROOT}"
                                                          "${REPOSITORY_ROOT}/c/vendor/")
target_link_libraries(adbc_validation PUBLIC nanoarrow GTest::gtest GTest::gmock)

set_target_properties(adbc_validation
                      PROPERTIES PUBLIC_HEADER "adbc_validation.h;adbc_validation_util.h")

install(TARGETS adbc_validation
        EXPORT AdbcValidationTargets
        LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
        PUBLIC_HEADER DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/adbc_validation/")

write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/AdbcValidation/AdbcValidationConfigVersion.cmake"
  COMPATIBILITY AnyNewerVersion)
export(EXPORT AdbcValidationTargets
       FILE "${CMAKE_CURRENT_BINARY_DIR}/AdbcValidation/AdbcValidationTargets.cmake"
       NAMESPACE AdbcValidation::)
configure_package_config_file("AdbcValidationConfig.cmake.in"
                              "${CMAKE_CURRENT_BINARY_DIR}/AdbcValidation/AdbcValidationConfig.cmake"
                              INSTALL_DESTINATION "AdbcValidation/AdbcValidationConfig.cmake"
)

install(EXPORT AdbcValidationTargets
        FILE AdbcValidationTargets.cmake
        NAMESPACE AdbcValidation::
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AdbcValidation")
install(FILES "${CMAKE_CURRENT_BINARY_DIR}/AdbcValidation/AdbcValidationConfig.cmake"
              "${CMAKE_CURRENT_BINARY_DIR}/AdbcValidation/AdbcValidationConfigVersion.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/AdbcValidation"
        COMPONENT Devel)
