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


golang = find_program('go')

if build_machine.system() == 'windows'
  prefix = ''
  suffix = '.lib'
elif build_machine.system() == 'darwin'
  prefix = 'lib'
  suffix = '.dylib'
else
  prefix = 'lib'
  suffix = '.so'
endif

adbc_driver_bigquery_name = prefix + 'adbc_driver_bigquery' + suffix
adbc_driver_bigquery_lib = custom_target(
    'adbc_driver_bigquery',
    output: adbc_driver_bigquery_name,
    command : [
        golang,
        'build',
        '-C',
        meson.project_source_root() + '/../go/adbc/pkg/bigquery',
        '-tags=driverlib',
        '-buildmode=c-shared',
        '-o',
        meson.current_build_dir() + '/' + adbc_driver_bigquery_name,
    ],
    install : true,
    install_dir : '.',
)

pkg.generate(
    name: 'Apache Arrow Database Connectivity (ADBC) BigQuery driver',
    description: 'The ADBC BigQuery driver provides an ADBC driver for BigQuery.',
    libraries: [adbc_driver_bigquery_lib],
    url: 'https://github.com/apache/arrow-adbc',
    filebase: 'adbc-driver-bigquery',
)

if get_option('tests')
    exc = executable(
        'adbc-driver-bigquery-test',
        'bigquery_test.cc',
        include_directories: [root_dir, driver_dir],
        link_with: [
            adbc_common_lib,
            adbc_driver_bigquery_lib
        ],
        dependencies: [adbc_validation_dep],
    )
    test('adbc-driver-bigquery', exc)
endif
