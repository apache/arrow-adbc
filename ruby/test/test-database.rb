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

class DatabaseTest < Test::Unit::TestCase
  sub_test_case(".open") do
    test("applies LoadFlags::DEFAULT when load_flags is not passed") do
      assert_nothing_raised do
        ADBC::Database.open(driver: "adbc_driver_sqlite", uri: ":memory:") do |_database|
        end
      end
    end

    test("load_flags: none disables all search paths") do
      error = assert_raise_kind_of(ADBC::Error) do
        ADBC::Database.open(driver: "adbc_driver_sqlite",
                            uri: ":memory:",
                            load_flags: ADBC::LoadFlags.new(0)) do |_database|
        end
      end
      assert_match(/not enabled at run time/, error.message)
    end
  end
end
