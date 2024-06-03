# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

if get_option('tests')
    duckdb_test = executable(
        'adbc_integration_duckdb_test',
        'duckdb_test.cc',
        include_directories: [root_dir, driver_dir],
        link_with: [
            adbc_common_lib,
            adbc_validation_lib,
            adbc_driver_manager_lib,
        ],
        dependencies: [nanoarrow_dep, gtest_main_dep, gmock_dep, duckdb_dep],
    )
    test('adbc-integration-duckdb', duckdb_test)
endif
