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

if host_machine.system() != 'windows'
    dl_dep = dependency('dl')
else
    dl_dep = declare_dependency()
endif

adbc_driver_manager_lib = library(
    'adbc_driver_manager',
    'adbc_driver_manager.cc',
    include_directories: [include_dir, vendor_dir],
    install: true,
    dependencies: [dl_dep],
)

pkg.generate(
    name: 'Apache Arrow Database Connectivity (ADBC) driver manager',
    description: 'ADBC driver manager provides API to use ADBC driver.',
    url: 'https://github.com/apache/arrow-adbc',
    libraries: [adbc_driver_manager_lib],
    filebase: 'adbc-driver-manager',
)

adbc_driver_manager_dep = declare_dependency(
    include_directories: include_dir,
    link_with: adbc_driver_manager_lib,
)
