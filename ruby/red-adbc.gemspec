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
  spec.version = ADBC::VERSION
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
  spec.extensions = ["dependency-check/Rakefile"]

  spec.add_runtime_dependency("red-arrow")

  # spec.metadata["msys2_mingw_dependencies"] = "adbc>=#{spec.version}"
end
