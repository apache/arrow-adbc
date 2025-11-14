# -*- ruby -*-
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

require "fileutils"

require_relative "lib/adbc/version"

Gem::Specification.new do |spec|
  spec.name = "red-adbc"
  version_components = [
    ADBC::Version::MAJOR.to_s,
    ADBC::Version::MINOR.to_s,
    ADBC::Version::MICRO.to_s,
    ADBC::Version::TAG,
  ]
  spec.version = version_components.compact.join(".")
  spec.homepage = "https://arrow.apache.org/"
  spec.authors = ["Apache Arrow Developers"]
  spec.email = ["dev@arrow.apache.org"]

  spec.summary =
    "Red ADBC is the Ruby bindings of ADBC (Apache Arrow Database Connectivity)"
  spec.description =
    "ADBC (Apache Arrow Database Connectivity) is an API standard for " +
    "database access libraries that uses Apache Arrow for data."
  spec.license = "Apache-2.0"
  spec.files = ["README.md"]
  shared_files = [
    "LICENSE.txt",
    "NOTICE.txt",
  ]
  shared_files.each do |file|
    FileUtils.cp("../#{file}", file)
    spec.files += [file]
  end
  spec.files += Dir.glob("lib/**/*.rb")

  # For CI: sometimes we can't use the latest red-arrow due to
  # conda-forge arrow-c-glib lagging behind
  if ENV["RED_ARROW_VERSION"]
    spec.add_runtime_dependency("red-arrow", ENV["RED_ARROW_VERSION"])
  else
    spec.add_runtime_dependency("red-arrow")
  end

  required_adbc_glib_version = version_components[0, 3].join(".")
  [
    ["debian", "libadbc-glib-dev"],
    ["rhel", "adbc-glib-devel"],
  ].each do |platform, package|
    spec.requirements <<
      "system: adbc-glib>=#{required_adbc_glib_version}: " +
      "#{platform}: #{package}"
  end

  # spec.metadata["msys2_mingw_dependencies"] = \
  #   "adbc>=#{required_adbc_glib_version}"
end
