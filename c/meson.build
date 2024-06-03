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
    'c', 'cpp',
    version: '0.6.0-SNAPSHOT',  # TODO: can this be dynamic?
    license: 'Apache 2.0',
    meson_version: '>=1.3.0',
    default_options: [
        'buildtype=release',
        'c_std=c99',
        'warning_level=2',
        'cpp_std=c++17',
    ]
)

root_dir = include_directories('..')
driver_dir = include_directories('driver')
nanoarrow_dep = dependency('nanoarrow')
fmt_dep = dependency('fmt')

if get_option('tests')
    gtest_main_dep = dependency('gtest_main')
    gtest_dep = dependency('gtest')
    gmock_dep = dependency('gmock')
else
    gtest_dep = disabler()
    gmock_dep = disabler()
endif

if get_option('duckdb')
    cmake = import('cmake')
    opt_var = cmake.subproject_options()
    # Doesn't appear duckdb properly builds PIC shared libraries
    # https://github.com/mesonbuild/meson/issues/10764
    # Adding these args is a dirty hack
    compiler_id = meson.get_compiler('c').get_id()
    if compiler_id == 'gcc' or compiler_id == 'clang'
        opt_var.append_compile_args('c', '-fPIC')
        opt_var.append_compile_args('cpp', '-fPIC')
    endif
    duckdb_proj = cmake.subproject('duckdb', options: opt_var)
    duckdb_dep = duckdb_proj.dependency('duckdb')
else
    duckdb_dep = disabler()
endif

needs_driver_manager = get_option('driver_manager') \
    or get_option('duckdb') \
    or get_option('tests')

if needs_driver_manager
    subdir('driver_manager')
endif

subdir('driver/common')
subdir('driver/framework')

if get_option('tests')
  subdir('validation')
endif

if get_option('flightsql')
  error(
      '''Meson does not yet support Golang - see
      https://github.com/mesonbuild/meson/issues?q=golang+'''
  )
endif

if get_option('postgresql')
  subdir('driver/postgresql')
endif

if get_option('sqlite')
  subdir('driver/sqlite')
endif

if get_option('snowflake')
  error(
      '''Meson does not yet support Golang - see
      https://github.com/mesonbuild/meson/issues?q=golang+'''
  )
endif

if get_option('duckdb')
  # TODO: currently broken because #include <adbc.h> conflicts between duckdb /
  # arrow-adbc includes
  subdir('integration/duckdb')
endif

if get_option('python')
  # TODO: implement (?) or should we make python use meson-python
  # and install appropriate subproject dependencies? leaning towards latter
endif
