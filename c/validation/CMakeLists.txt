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

add_library(adbc_validation_util STATIC adbc_validation_util.cc)
adbc_configure_target(adbc_validation_util)
target_compile_features(adbc_validation_util PRIVATE cxx_std_17)
target_include_directories(adbc_validation_util SYSTEM
                           PRIVATE "${REPOSITORY_ROOT}/c/include/"
                                   "${REPOSITORY_ROOT}/c/driver/"
                                   "${REPOSITORY_ROOT}/c/vendor/")
target_link_libraries(adbc_validation_util PUBLIC adbc_driver_common nanoarrow
                                                  GTest::gtest GTest::gmock)

add_library(adbc_validation OBJECT
            adbc_validation.cc adbc_validation_connection.cc adbc_validation_database.cc
            adbc_validation_statement.cc)
adbc_configure_target(adbc_validation)
target_compile_features(adbc_validation PRIVATE cxx_std_17)
target_include_directories(adbc_validation SYSTEM
                           PRIVATE "${REPOSITORY_ROOT}/c/include/"
                                   "${REPOSITORY_ROOT}/c/driver/"
                                   "${REPOSITORY_ROOT}/c/vendor/")
target_link_libraries(adbc_validation
                      PUBLIC adbc_driver_common
                             adbc_validation_util
                             nanoarrow
                             GTest::gtest
                             GTest::gmock)
