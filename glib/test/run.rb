#!/usr/bin/env ruby
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

ENV["TEST_UNIT_MAX_DIFF_TARGET_STRING_SIZE"] ||= "10000"

require "pathname"
require "test-unit"

(ENV["ADBC_DLL_PATH"] || "").split(File::PATH_SEPARATOR).each do |path|
  RubyInstaller::Runtime.add_dll_directory(path)
end

base_dir = Pathname(__dir__).parent
test_dir = base_dir + "test"

require "gi"

ADBC = GI.load("ADBC")
begin
  ADBCArrow = GI.load("ADBCArrow")
rescue GObjectIntrospection::RepositoryError => error
  puts("ADBCArrow isn't found: #{error}")
end

require_relative "helper"
require_relative "helper/sandbox"

exit(Test::Unit::AutoRunner.run(true, test_dir.to_s))
