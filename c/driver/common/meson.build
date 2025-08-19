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

adbc_common_lib = library(
    'adbc_driver_common',
    sources: ['utils.c'],
    include_directories: [include_dir],
    dependencies: [nanoarrow_dep],
    install: true,
)

exc = executable(
    'adbc-driver-common-test',
    'utils_test.cc',
    include_directories: [include_dir],
    link_with: [adbc_common_lib],
    dependencies: [nanoarrow_dep, gtest_main_dep, gmock_dep],
)
test('adbc-driver-common', exc)
