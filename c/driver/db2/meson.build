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

db2_dep = meson.get_compiler('c').find_library('db2', required: true)

adbc_db2_driver_lib = library(
    'adbc_driver_db2',
    sources: [
        'connection.cc',
        'database.cc',
        'db2.cc',
        'error.cc',
    ],
    include_directories: [include_dir, c_dir, driver_dir],
    link_with: [adbc_common_lib, adbc_framework_lib],
    dependencies: [nanoarrow_dep, fmt_dep, db2_dep],
)

pkg.generate(
    name: 'Apache Arrow Database Connectivity (ADBC) Db2 driver',
    description: 'The ADBC Db2 driver provides an ADBC driver for IBM Db2 LUW.',
    url: 'https://github.com/apache/arrow-adbc',
    libraries: [adbc_db2_driver_lib],
    filebase: 'adbc-driver-db2',
)

adbc_driver_db2_dep = declare_dependency(
    include_directories: include_dir,
    link_with: adbc_db2_driver_lib,
)
