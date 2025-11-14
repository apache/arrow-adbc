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

class ConnectionTest < Test::Unit::TestCase
  def setup
    options = {
      driver: "adbc_driver_sqlite",
      uri: ":memory:",
    }
    ADBC::Database.open(**options) do |database|
      connect_options = {}
      database.connect(**connect_options) do |connection|
        @connection = connection
        yield
      end
    end
  end

  sub_test_case("#query") do
    def test_block
      @connection.query("SELECT 1") do |reader, n_rows_affected|
        assert_equal([
                       Arrow::Table.new("1" => Arrow::Int64Array.new([1])),
                       -1,
                     ],
                     [
                       reader.read_all,
                       n_rows_affected,
                     ])
      end
    end

    def test_no_block
      assert_equal([
                     Arrow::Table.new("1" => Arrow::Int64Array.new([1])),
                     -1,
                   ],
                   @connection.query("SELECT 1"))
    end
  end

  sub_test_case("#info") do
    def test_all
      info = @connection.info
      [
        :vendor_version,
        :driver_arrow_version,
      ].each do |version_name|
        next unless info.key?(version_name)
        info[version_name] = normalize_version(info[version_name])
      end
      assert_equal({
                     vendor_name: "SQLite",
                     vendor_version: "X.Y.Z",
                     driver_name: "ADBC SQLite Driver",
                     driver_version: "(unknown)",
                     driver_arrow_version: "X.Y.Z"
                   },
                   info)
    end

    def test_integer
      assert_equal({vendor_name: "SQLite"},
                   @connection.info([ADBC::Info::VENDOR_NAME]))
    end

    def test_symbol
      assert_equal({vendor_name: "SQLite"},
                   @connection.info([:vendor_name]))
    end

    def test_STRING
      assert_equal({vendor_name: "SQLite"},
                   @connection.info(["VENDOR_NAME"]))
    end
  end

  def test_get_objects
    assert_equal([
                   [
                     "main",
                     [
                       {
                         "db_schema_name" => "",
                         "db_schema_tables" => [],
                       },
                     ],
                   ],
                 ],
                 @connection.get_objects.raw_records)
  end

  def test_vendor_name
    assert_equal("SQLite", @connection.vendor_name)
  end

  def test_vendor_version
    assert_equal("X.Y.Z", normalize_version(@connection.vendor_version))
  end

  def test_vendor_arrow_version
    assert_equal(nil, normalize_version(@connection.vendor_arrow_version))
  end

  def test_driver_name
    assert_equal("ADBC SQLite Driver", @connection.driver_name)
  end

  def test_driver_version
    assert_equal("(unknown)", normalize_version(@connection.driver_version))
  end

  def test_driver_arrow_version
    assert_equal("X.Y.Z", normalize_version(@connection.driver_arrow_version))
  end

  private
  def normalize_version(version)
    version&.gsub(/\A\d+\.\d+\.\d+(?:-SNAPSHOT)?\z/, "X.Y.Z")
  end
end
