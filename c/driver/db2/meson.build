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

db2_dep = dependency('odbc', required: false)
if not db2_dep.found()
  db2_dep = meson.get_compiler('c').find_library('db2', required: false)
  if not db2_dep.found()
    db2_dep = meson.get_compiler('c').find_library('odbc', required: true)
  endif
endif

adbc_db2_driver_lib = library(
    'adbc_driver_db2',
    sources: [
        'db2.cc',
        'database.cc',
        'connection.cc',
        'statement.cc',
        'result_reader.cc',
        'type_mapping.cc',
        'error.cc',
    ],
    include_directories: [include_dir, c_dir, driver_dir],
    link_with: [adbc_common_lib, adbc_framework_lib],
    dependencies: [nanoarrow_dep, fmt_dep, db2_dep],
)

pkg.generate(
    name: 'Apache Arrow Database Connectivity (ADBC) DB2 driver',
    description: 'The ADBC DB2 driver provides an ADBC driver for IBM DB2.',
    url: 'https://github.com/apache/arrow-adbc',
    libraries: [adbc_db2_driver_lib],
    filebase: 'adbc-driver-db2',
)

adbc_driver_db2_dep = declare_dependency(
    include_directories: include_dir,
    link_with: adbc_db2_driver_lib,
)
