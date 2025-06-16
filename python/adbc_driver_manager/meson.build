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
    'adbc_driver_manager',
    'cython',
    version: run_command(
        ['python', 'adbc_driver_manager/_version.py'],
        check: true,
    ).stdout().replace(
        'Version:',
        '',
    ).strip(),
    license: 'Apache-2.0',
    meson_version: '>=1.2.1',
    default_options: [
        'buildtype=release',
        'warning_level=2',
        'cpp_std=c++17',
        # conda environments may only provide a static library for
        # libbfd, which backward-cpp requires.
        'default_library=static',
    ],
)

backward_dep = dependency('backward-cpp')
# hack until https://github.com/mesonbuild/wrapdb/pull/2072
backward_incdir = include_directories('subprojects/backward-cpp-1.6')

adbc_driver_manager_dep = dependency(
    'adbc-driver-manager',
    fallback: ['arrow-adbc', 'adbc_driver_manager_dep'],
    default_options: ['driver_manager=enabled'],
)
py = import('python').find_installation(pure: false)

cython_args = []
if get_option('buildtype') in ['debug', 'debugoptimized']
    cython_args += ['--gdb']
endif

subdir('adbc_driver_manager')

meson.add_dist_script(py, files('scripts/remove_third_party_wraps.py'))
