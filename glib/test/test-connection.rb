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
    @database = ADBC::Database.new
    @database.set_option("driver", "adbc_driver_sqlite")
    @database.set_option("uri", ":memory:")
    @database.init
    @connection = ADBC::Connection.new
    begin
      @connection.init(@database)
      yield
    ensure
      @connection.release
    end
  end

  def test_table_types
    c_abi_array_stream = @connection.table_types
    begin
      reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
      table = reader.read_all
      fields = [
        Arrow::Field.new("table_type", :string, false),
      ]
      schema = Arrow::Schema.new(fields)
      table_types = Arrow::StringArray.new(["table", "view"])
      assert_equal(Arrow::Table.new(schema, [table_types]),
                   table)
    ensure
      GLib.free(c_abi_array_stream)
    end
  end
end
