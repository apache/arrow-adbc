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

project(adbc_cookbook_recipes
        VERSION "1.0.0"
        LANGUAGES CXX)

include(CTest)
include(FetchContent)

set(CMAKE_CXX_STANDARD 17)

find_package(AdbcDriverManager REQUIRED)

fetchcontent_declare(nanoarrow
                     GIT_REPOSITORY https://github.com/apache/arrow-nanoarrow.git
                     GIT_TAG apache-arrow-nanoarrow-0.2.0
                     GIT_SHALLOW TRUE
                     EXCLUDE_FROM_ALL)
fetchcontent_makeavailable(nanoarrow)

add_executable(recipe-quickstart quickstart.cc)
target_include_directories(recipe-quickstart SYSTEM PRIVATE ${nanoarrow_SOURCE_DIR}/dist)
target_link_libraries(recipe-quickstart
                      PRIVATE AdbcDriverManager::adbc_driver_manager_shared nanoarrow)
add_test(NAME quickstart COMMAND recipe-quickstart)
install(TARGETS recipe-quickstart)
