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

project(adbc-static-test LANGUAGES C CXX)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

find_package(AdbcDriverCommon REQUIRED)
find_package(AdbcDriverFramework REQUIRED)

find_package(AdbcDriverBigQuery REQUIRED)
find_package(AdbcDriverFlightSQL REQUIRED)
find_package(AdbcDriverPostgreSQL REQUIRED)
find_package(AdbcDriverSQLite REQUIRED)
find_package(AdbcDriverSnowflake REQUIRED)

find_package(fmt REQUIRED)
find_package(nanoarrow REQUIRED)

add_executable(static_test main.c)
# We need to link the C++ standard library since some of the drivers are
# written in C++.
set_target_properties(static_test PROPERTIES LINKER_LANGUAGE CXX)
target_link_libraries(static_test
                      PRIVATE AdbcDriverFlightSQL::adbc_driver_flightsql_static
                              AdbcDriverPostgreSQL::adbc_driver_postgresql_static
                              AdbcDriverSQLite::adbc_driver_sqlite_static
                              PostgreSQL::PostgreSQL
                              SQLite::SQLite3
                              fmt::fmt
                              nanoarrow::nanoarrow
                              AdbcDriverCommon::adbc_driver_common
                              AdbcDriverFramework::adbc_driver_framework)
