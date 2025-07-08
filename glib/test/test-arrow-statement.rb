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

class ArrowStatementTest < Test::Unit::TestCase
  include Helper

  def setup
    omit("adbc-arrow-glib isn't built") unless defined?(ADBCArrow)

    @database = ADBC::Database.new
    @database.set_option("driver", "adbc_driver_sqlite")
    @database.set_option("uri", ":memory:")
    @database.init
    @connection = ADBC::Connection.new
    @connection.init(@database)
    @statement = ADBCArrow::Statement.new(@connection)
  end

  def teardown
    @statement&.release
  end

  def test_execute
    @statement.set_sql_query("SELECT 1")
    _, reader, _ = @statement.execute(true)
    assert_equal(Arrow::Table.new("1" => Arrow::Int64Array.new([1])),
                 reader.read_all)
  end

  def test_parameter_schema
    @statement.set_sql_query("SELECT ?, $2")
    @statement.prepare
    assert_equal(Arrow::Schema.new("0" => :null,
                                   "$2" => :null),
                 @statement.parameter_schema)
  end

  def test_bind
    @statement.set_sql_query("CREATE TABLE data (number int)")
    @statement.execute(false)

    record_batch =
      Arrow::RecordBatch.new(number: Arrow::Int64Array.new([10, 20, 30]))
    @statement.ingest_target_table = "data"
    @statement.ingest_mode = :append
    @statement.bind(record_batch)
    _, _, n_rows_affected = @statement.execute(false)
    assert_equal(3, n_rows_affected)

    @statement.set_sql_query("SELECT * FROM data")
    _, reader, _ = @statement.execute(true)
    assert_equal(record_batch.to_table,
                 reader.read_all)
  end

  def test_bind_stream
    @statement.set_sql_query("CREATE TABLE data (number int)")
    @statement.execute(false)

    record_batch =
      Arrow::RecordBatch.new(number: Arrow::Int64Array.new([10, 20, 30]))
    @statement.ingest_target_table = "data"
    @statement.ingest_mode = :append
    reader = Arrow::RecordBatchReader.new([record_batch])
    @statement.bind_stream(reader)
    _, _, n_rows_affected = @statement.execute(false)
    assert_equal(3, n_rows_affected)

    @statement.set_sql_query("SELECT * FROM data")
    _, reader, _ = @statement.execute(true)
    assert_equal(record_batch.to_table,
                 reader.read_all)
  end
end
