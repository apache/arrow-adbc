# -*- indent-tabs-mode: nil -*-
#
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

project(
    'adbc-glib',
    'c',
    default_options: ['c_std=c99', 'cpp_std=c++17'],
    license: 'Apache-2.0',
    version: '1.11.0-SNAPSHOT',
)

version_numbers = meson.project_version().split('-')[0].split('.')
version_major = version_numbers[0].to_int()
version_minor = version_numbers[1].to_int()
version_micro = version_numbers[2].to_int()

api_version = '1.0'
so_version = version_major
library_version = '.'.join(version_numbers)

prefix = get_option('prefix')
include_dir = get_option('includedir')
project_include_sub_dir = meson.project_name()
data_dir = get_option('datadir')
gir_dir = prefix / data_dir / 'gir-1.0'
vapi_dir = data_dir / 'vala' / 'vapi'

gnome = import('gnome')
pkgconfig = import('pkgconfig')
pkgconfig_variables = ['girdir=@0@'.format(gir_dir)]

base_include_directories = [
    include_directories('.'),
    include_directories('..' / 'c' / 'include'),
]

gi_fatal_warnings = (build_machine.system() != 'windows')

adbc_build_dir = get_option('adbc_build_dir')
if adbc_build_dir != ''
    adbc_build_dir = meson.source_root() / adbc_build_dir
endif

c_compiler = meson.get_compiler('c')
if adbc_build_dir == ''
    adbc_driver_manager = dependency('adbc-driver-manager')
else
    adbc_driver_manager = c_compiler.find_library(
        'adbc_driver_manager',
        dirs: [adbc_build_dir],
    )
endif

build_example = get_option('example')

dependency('gobject-introspection-1.0', required: false).found()
generate_vapi = get_option('vapi')
if generate_vapi
    pkgconfig_variables += ['vapidir=@0@'.format(vapi_dir)]
    add_languages('vala')
endif

# Specify -fPIE explicitly for g-ir-scanner because PIE is disabled by
# default on AlmaLinux 9 RPM build by
# LDFLAGS="-specs=/usr/lib/rpm/redhat/redhat-hardened-ld".
gir_scanner_cflags = c_compiler.get_supported_arguments('-fPIE')
if gir_scanner_cflags.length() == 0
    gir_scanner_extra_args = []
else
    gir_scanner_extra_args = ['--cflags-begin'] + gir_scanner_cflags + [
        '--cflags-end',
    ]
endif

python = import('python')
python3 = python.find_installation('python3')
generate_version_header_py = meson.project_source_root() / 'tool' / 'generate-version-header.py'

subdir('adbc-glib')
arrow_glib = dependency('arrow-glib', required: generate_vapi)
if arrow_glib.found()
    subdir('adbc-arrow-glib')
endif
subdir('example')

install_data(
    '../LICENSE.txt',
    'README.md',
    install_dir: data_dir / 'doc' / meson.project_name(),
)
if arrow_glib.found()
    install_data(
        '../LICENSE.txt',
        'README.md',
        install_dir: data_dir / 'doc' / 'adbc-arrow-glib',
    )
endif

ruby = find_program('ruby', required: false)
if ruby.found()
    test_run_rb = files('test/run.rb')
    test('unit test', ruby, args: [test_run_rb])
endif
