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

py.extension_module(
    '_backward',
    sources: ['_backward.pyx'],
    dependencies: [backward_dep],
    include_directories: backward_incdir,
    override_options: ['cython_language=cpp'],
    install: true,
    subdir: 'adbc_driver_manager',
)

py.extension_module(
    '_lib',
    sources: ['_blocking_impl.cc', '_lib.pyx'],
    dependencies: [adbc_driver_manager_dep],
    override_options: ['cython_language=cpp'],
    install: true,
    subdir: 'adbc_driver_manager',
)

py.extension_module(
    '_reader',
    sources: ['_reader.pyx'],
    dependencies: [adbc_driver_manager_dep],
    override_options: ['cython_language=cpp'],
    install: true,
    subdir: 'adbc_driver_manager',
)
