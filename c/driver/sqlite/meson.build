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

sqlite3_dep = dependency('sqlite3')

time_t_size = meson.get_compiler('c').sizeof(
    'time_t',
    prefix: '#include <sys/types.h>',
)

adbc_sqlite3_driver_lib = library(
    'adbc_driver_sqlite',
    sources: ['sqlite.cc', 'statement_reader.c'],
    include_directories: [include_dir, c_dir],
    link_with: [adbc_common_lib],
    dependencies: [adbc_framework_dep, nanoarrow_dep, fmt_dep, sqlite3_dep],
    c_args: ['-DSIZEOF_TIME_T=' + time_t_size.to_string()],
    gnu_symbol_visibility: 'hidden',
    cpp_static_args: adbc_cpp_static_args,
)

pkg.generate(
    name: 'Apache Arrow Database Connectivity (ADBC) SQLite driver',
    description: 'The ADBC SQLite driver provides an ADBC driver for SQLite.',
    url: 'https://github.com/apache/arrow-adbc',
    libraries: [adbc_sqlite3_driver_lib],
    filebase: 'adbc-driver-sqlite',
    variables: ['Cflags.private=-DADBC_STATIC'],
)

adbc_driver_sqlite_dep = declare_dependency(
    include_directories: include_dir,
    link_with: adbc_sqlite3_driver_lib,
)

exc = executable(
    'adbc-driver-sqlite-test',
    sources: ['sqlite_test.cc'],
    include_directories: [include_dir, c_dir, driver_dir],
    link_with: [adbc_common_lib, adbc_sqlite3_driver_lib],
    dependencies: [sqlite3_dep, adbc_validation_dep],
)
test('adbc-driver-sqlite', exc)
