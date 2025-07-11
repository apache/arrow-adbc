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

project(
    'arrow-adbc',
    'c',
    'cpp',
    version: '1.8.0-SNAPSHOT',
    license: 'Apache-2.0',
    meson_version: '>=1.3.0',
    default_options: [
        'buildtype=release',
        'c_std=c99',
        'warning_level=2',
        'cpp_std=c++17',
    ],
)

add_project_arguments(
    '-Wno-int-conversion',
    '-Wno-unused-parameter',
    language: 'c',
)
add_project_arguments('-Wno-unused-parameter', '-Wno-reorder', language: 'cpp')

c_dir = include_directories('.')
vendor_dir = include_directories('vendor')
include_dir = include_directories('include')
install_headers('include/adbc.h')
install_headers('include/arrow-adbc/adbc.h', subdir: 'arrow-adbc')
driver_dir = include_directories('driver')
nanoarrow_dep = dependency('nanoarrow')
fmt_dep = dependency('fmt')

if get_option('tests').enabled()
    gtest_main_dep = dependency('gtest_main')
    gmock_dep = dependency('gmock')
else
    gtest_main_dep = disabler()
    gmock_dep = disabler()
endif

needs_driver_manager = get_option('driver_manager').enabled() \
    or get_option('tests').enabled()

pkg = import('pkgconfig')

if needs_driver_manager
    install_headers('include/adbc_driver_manager.h')
    install_headers(
        'include/arrow-adbc/adbc_driver_manager.h',
        subdir: 'arrow-adbc',
    )
    subdir('driver_manager')
endif

subdir('driver/common')
subdir('driver/framework')
subdir('validation')

if get_option('bigquery').enabled()
    subdir('driver/bigquery')
endif

if get_option('flightsql').enabled()
    subdir('driver/flightsql')
endif

if get_option('postgresql').enabled()
    subdir('driver/postgresql')
endif

if get_option('sqlite').enabled()
    subdir('driver/sqlite')
endif

if get_option('snowflake').enabled()
    subdir('driver/snowflake')
endif
