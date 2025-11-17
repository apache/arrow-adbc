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

libpq_dep = dependency('libpq')

adbc_postgres_driver_lib = library(
    'adbc_driver_postgresql',
    sources: [
        'connection.cc',
        'error.cc',
        'database.cc',
        'postgresql.cc',
        'result_helper.cc',
        'result_reader.cc',
        'statement.cc',
    ],
    include_directories: [include_dir, c_dir, safe_math_dir],
    link_with: [adbc_common_lib, adbc_framework_lib],
    dependencies: [nanoarrow_dep, fmt_dep, libpq_dep],
)

pkg.generate(
    name: 'Apache Arrow Database Connectivity (ADBC) PostgreSQL driver',
    description: 'The ADBC PostgreSQL driver provides an ADBC driver for PostgreSQL.',
    url: 'https://github.com/apache/arrow-adbc',
    libraries: [adbc_postgres_driver_lib],
    filebase: 'adbc-driver-postgresql',
)

adbc_driver_postgresql_dep = declare_dependency(
    include_directories: include_dir,
    link_with: adbc_postgres_driver_lib,
)

postgres_tests = {
    'driver-postgresql': {
        'src_name': 'driver_postgresql',
        'sources': ['postgres_type_test.cc', 'postgresql_test.cc'],
    },
    'driver-postgresql-copy': {
        'src_name': 'driver_postgresql_copy',
        'sources': [
            'copy/postgres_copy_reader_test.cc',
            'copy/postgres_copy_writer_test.cc',
        ],
    },
}

foreach name, conf : postgres_tests
    exc = executable(
        'adbc-' + name + '-test',
        sources: conf['sources'],
        include_directories: [include_dir, driver_dir, c_dir],
        link_with: [adbc_common_lib, adbc_postgres_driver_lib],
        dependencies: [libpq_dep, adbc_validation_dep],
    )
    test('adbc-' + name, exc)
endforeach
