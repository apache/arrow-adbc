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
      @connection.query("SELECT 1") do |reader, n_affected_rows|
        assert_equal([
                       Arrow::Table.new("1" => Arrow::Int64Array.new([1])),
                       -1,
                     ],
                     [
                       reader.read_all,
                       n_affected_rows,
                     ])
      end
    end

    def test_no_block
      table = @connection.query("SELECT 1")
      assert_equal(Arrow::Table.new("1" => Arrow::Int64Array.new([1])),
                   table)
    end
  end
end
