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

sources = files(
  'connection.c',
  'statement.c',
)

definition_headers = files(
  'connection.h',
  'statement.h',
)

version_h_conf = configuration_data()
version_h_conf.set('GADBC_ARROW_VERSION', meson.project_version())
version_h_conf.set('GADBC_ARROW_VERSION_MAJOR', version_major)
version_h_conf.set('GADBC_ARROW_VERSION_MINOR', version_minor)
version_h_conf.set('GADBC_ARROW_VERSION_MICRO', version_micro)
version_h = configure_file(input: 'version.h.in',
                           output: 'version.h',
                           configuration: version_h_conf)
headers += version_h

headers = definition_headers
headers += files(
  'adbc-arrow-glib.h',
)

enums = gnome.mkenums_simple('enum-types',
                             identifier_prefix: 'GADBCArrow',
                             sources: definition_headers,
                             symbol_prefix: 'gadbc_arrow')
enums_header = enums[1]

install_headers(headers, subdir: 'adbc-arrow-glib')


dependencies = [
  adbc_glib,
  arrow_glib,
]
libadbc_arrow_glib = library('adbc-arrow-glib',
                             c_args: '-DG_LOG_DOMAIN="ADBC-Arrow"',
                             sources: sources + enums,
                             install: true,
                             dependencies: dependencies,
                             include_directories: base_include_directories,
                             soversion: so_version,
                             version: library_version)
adbc_arrow_glib = \
  declare_dependency(link_with: libadbc_arrow_glib,
                     include_directories: base_include_directories,
                     dependencies: dependencies,
                     sources: enums_header)

pkgconfig.generate(libadbc_arrow_glib,
                   description: 'Arrow GLib integration API for ADBC GLib',
                   filebase: 'adbc-arrow-glib',
                   name: 'ADBC Arrow GLib',
                   requires: ['adbc-glib', 'arrow-glib'],
                   variables: pkgconfig_variables,
                   version: meson.project_version())

adbc_arrow_glib_gir = \
  gnome.generate_gir(libadbc_arrow_glib,
                     dependencies: [
                       declare_dependency(sources: adbc_glib_gir),
                       arrow_glib,
                     ],
                     export_packages: 'adbc-arrow-glib',
                     extra_args: [
                       '--warn-all',
                     ],
                     fatal_warnings: gi_fatal_warnings,
                     header: 'adbc-arrow-glib/adbc-arrow-glib.h',
                     identifier_prefix: 'GADBCArrow',
                     includes: [
                       'ADBC-1.0',
                       'Arrow-1.0',
                     ],
                     install: true,
                     namespace: 'ADBCArrow',
                     nsversion: api_version,
                     sources: sources + definition_headers + enums,
                     symbol_prefix: 'gadbc_arrow')
if generate_vapi
  adbc_arrow_glib_vapi = \
    gnome.generate_vapi('adbc-arrow-glib',
                        gir_dirs: [
                          arrow_glib.get_variable('girdir'),
                        ],
                        install: true,
                        packages: [
                          adbc_glib_vapi,
                          'arrow-glib',
                        ],
                        sources: [adbc_arrow_glib_gir[0]],
                        vapi_dirs: [
                          arrow_glib.get_variable('vapidir'),
                        ])
endif
