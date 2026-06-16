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
    sub_test_case("manifest") do
      def setup
        Dir.mktmpdir do |tmpdir|
          File.write(File.join(tmpdir, "testdriver.toml"), <<~TOML)
            name = "test driver"
            version = "0.1.0"

            [Driver]
            shared = "adbc_driver_sqlite"
          TOML
          driver_path, ENV["ADBC_DRIVER_PATH"] = ENV["ADBC_DRIVER_PATH"], tmpdir
          begin
            yield
          ensure
            ENV["ADBC_DRIVER_PATH"] = driver_path
          end
        end
      end

      def test_search_env_finds_driver
        ADBC::Database.open(driver: "testdriver",
                            uri: ":memory:",
                            load_flags: :search_env) do |database|
          database.connect do |connection|
              assert_equal([
                              Arrow::Table.new("1" => Arrow::Int64Array.new([1])),
                              -1,
                           ],
                           connection.query("SELECT 1"))
          end
        end
      end

      def test_no_flags_cannot_find_driver
        assert_raise(ADBC::Error::NotFound) do
          ADBC::Database.open(driver: "testdriver",
                              uri: ":memory:",
                              load_flags: 0) do |_database|
          end
        end
      end
    end
  end
end
