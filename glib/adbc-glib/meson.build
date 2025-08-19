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

sources = files('connection.c', 'database.c', 'error.c', 'statement.c')

definition_headers = files(
    'connection.h',
    'database.h',
    'error.h',
    'statement.h',
)

version_h = configure_file(
    input: 'version.h.in',
    output: 'version.h',
    command: [
        python3,
        generate_version_header_py,
        '--library',
        'GADBC',
        '--version',
        meson.project_version(),
        '--input',
        '@INPUT@',
        '--output',
        '@OUTPUT@',
    ],
)
definition_headers += version_h

headers = definition_headers
headers += files(
    'adbc-glib-raw.h',
    'adbc-glib.h',
    'connection-raw.h',
    'database-raw.h',
    'error-raw.h',
    'statement-raw.h',
)

enums = gnome.mkenums_simple(
    'enum-types',
    identifier_prefix: 'GADBC',
    sources: definition_headers,
    symbol_prefix: 'gadbc',
)
enums_header = enums[1]

install_headers(headers, subdir: project_include_sub_dir)


gobject = dependency('gobject-2.0')
dependencies = [adbc_driver_manager, gobject]
libadbc_glib = library(
    'adbc-glib',
    c_args: '-DG_LOG_DOMAIN="ADBC"',
    sources: sources + enums,
    install: true,
    dependencies: dependencies,
    include_directories: base_include_directories,
    soversion: so_version,
    version: library_version,
)
adbc_glib = declare_dependency(
    link_with: libadbc_glib,
    include_directories: base_include_directories,
    dependencies: dependencies,
    sources: enums_header,
)

pkgconfig.generate(
    libadbc_glib,
    description: 'GLib API for ADBC driver manager',
    filebase: 'adbc-glib',
    name: 'ADBC GLib',
    requires: ['gobject-2.0', 'adbc-driver-manager'],
    variables: pkgconfig_variables,
    version: meson.project_version(),
)

adbc_glib_gir = gnome.generate_gir(
    libadbc_glib,
    export_packages: 'adbc-glib',
    extra_args: gir_scanner_extra_args,
    fatal_warnings: gi_fatal_warnings,
    header: 'adbc-glib/adbc-glib.h',
    identifier_prefix: 'GADBC',
    includes: ['GObject-2.0'],
    install: true,
    namespace: 'ADBC',
    nsversion: api_version,
    sources: sources + definition_headers + enums,
    symbol_prefix: 'gadbc',
)
if generate_vapi
    adbc_glib_vapi = gnome.generate_vapi(
        'adbc-glib',
        install: true,
        packages: ['gobject-2.0'],
        sources: [adbc_glib_gir[0]],
    )
endif
