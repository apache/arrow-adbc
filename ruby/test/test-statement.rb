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

class StatementTest < Test::Unit::TestCase
  def setup
    options = {
      driver: "adbc_driver_sqlite",
      uri: ":memory:",
    }
    ADBC::Database.open(**options) do |database|
      connect_options = {}
      database.connect(**connect_options) do |connection|
        connection.open_statement do |statement|
          @statement = statement
          yield
        end
      end
    end
  end

  test("#parameter_schema") do
    @statement.sql_query = "SELECT ?"
    @statement.prepare
    assert_equal(Arrow::Schema.new("0" => :null),
                 @statement.parameter_schema)
  end

  sub_test_case("#ingest") do
    test("Arrow::RecordBatch") do
      numbers = Arrow::Int64Array.new([10, 20, 30])
      record_batch = Arrow::RecordBatch.new(number: numbers)
      @statement.ingest("data", record_batch)
      table, n_rows_affected = @statement.query("SELECT * FROM data")
      assert_equal([
                     Arrow::Table.new(number: numbers),
                     -1,
                   ],
                   [
                     table,
                     n_rows_affected,
                   ])
    end

    test("Arrow::RecordBatchReader") do
      numbers = Arrow::Int64Array.new([10, 20, 30])
      record_batch = Arrow::RecordBatch.new(number: numbers)
      @statement.ingest("data", Arrow::RecordBatchReader.new([record_batch]))
      table, n_rows_affected = @statement.query("SELECT * FROM data")
      assert_equal([
                     Arrow::Table.new(number: numbers),
                     -1,
                   ],
                   [
                     table,
                     n_rows_affected,
                   ])
    end

    test("Arrow::Table") do
      numbers = Arrow::Int64Array.new([10, 20, 30])
      input_table = Arrow::Table.new(number: numbers)
      @statement.ingest("data", input_table)
      table, n_rows_affected = @statement.query("SELECT * FROM data")
      assert_equal([
                     input_table,
                     -1,
                   ],
                   [
                     table,
                     n_rows_affected,
                   ])
    end
  end
end
