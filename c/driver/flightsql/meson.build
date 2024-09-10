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

adbc_driver_flightsql_name = prefix + 'adbc_driver_flightsql' + suffix
adbc_driver_flightsql_lib = custom_target(
    'adbc_driver_flightsql',
    output: adbc_driver_flightsql_name,
    command : [
        golang,
        'build',
        '-C',
        meson.project_source_root() + '/../go/adbc/pkg/flightsql',
        '-tags=driverlib',
        '-buildmode=c-shared',
        '-o',
        meson.current_build_dir() + '/' + adbc_driver_flightsql_name,
    ],
    install : true,
    install_dir : '.',
)

pkg.generate(
    name: 'Apache Arrow Database Connectivity (ADBC) Flight SQL driver',
    description: 'The ADBC Flight SQL driver provides an ADBC driver for Flight SQL.',
    url: 'https://github.com/apache/arrow-adbc',
    libraries: [adbc_driver_flightsql_lib],
    filebase: 'adbc-driver-flightsql',
)

if get_option('tests')
    exc = executable(
        'adbc-driver-flightsql-test',
        'dremio_flightsql_test.cc',
        'sqlite_flightsql_test.cc',
        include_directories: [include_dir, c_dir, driver_dir],
        link_with: [
            adbc_common_lib,
            adbc_driver_flightsql_lib
        ],
        dependencies: [adbc_validation_dep],
    )
    test('adbc-driver-flightsql', exc)
endif
