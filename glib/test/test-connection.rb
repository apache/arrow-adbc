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
  include Helper

  def setup
    @database = ADBC::Database.new
    @database.set_option("driver", "adbc_driver_sqlite")
    Dir.mktmpdir do |tmp_dir|
      database = File.join(tmp_dir, "test.sqlite3")
      @database.set_option("uri", database)
      @database.init
      open_connection do |connection|
        @connection = connection
        yield
      end
    end
  end

  def open_connection
    connection = ADBC::Connection.new
    begin
      connection.init(@database)
      yield(connection)
    ensure
      connection.release
    end
  end

  def normalize_version(version)
    return nil if version.nil?
    version.gsub(/\A\d+\.\d+\.\d+(?:-SNAPSHOT)?\z/, "X.Y.Z")
  end

  def normalize_info(info)
    info.collect do |code, value|
      value = value.values[0] if value.is_a?(Hash)
      case code
      when ADBC::Info::VENDOR_VERSION,
           ADBC::Info::DRIVER_ARROW_VERSION
        value = normalize_version(value)
      end
      [code, value]
    end
  end

  sub_test_case("#get_info") do
    def test_all
      c_abi_array_stream = @connection.get_info
      begin
        reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
        table = reader.read_all
        assert_equal([
                       [ADBC::Info::VENDOR_NAME, "SQLite"],
                       [ADBC::Info::VENDOR_VERSION, "X.Y.Z"],
                       [ADBC::Info::DRIVER_NAME, "ADBC SQLite Driver"],
                       [ADBC::Info::DRIVER_VERSION, "(unknown)"],
                       [ADBC::Info::DRIVER_ARROW_VERSION, "X.Y.Z"],
                     ],
                     normalize_info(table.raw_records))
        ensure
        GLib.free(c_abi_array_stream)
      end
    end

    def test_multiple
      codes = [
        ADBC::Info::VENDOR_NAME,
        ADBC::Info::DRIVER_NAME,
      ]
      c_abi_array_stream = @connection.get_info(codes)
      begin
        reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
        table = reader.read_all
        assert_equal([
                       [ADBC::Info::VENDOR_NAME, "SQLite"],
                       [ADBC::Info::DRIVER_NAME, "ADBC SQLite Driver"],
                     ],
                     normalize_info(table.raw_records))
      ensure
        GLib.free(c_abi_array_stream)
      end
    end
  end

  def test_table_schema
    execute_sql(@connection,
                "CREATE TABLE data (number int, string text)",
                need_result: false)
    execute_sql(@connection,
                "INSERT INTO data VALUES (1, 'hello')",
                need_result: false)

    c_abi_schema = @connection.get_table_schema(nil, nil, "data")
    begin
      schema = Arrow::Schema.import(c_abi_schema)
      assert_equal(Arrow::Schema.new(number: :int64,
                                     string: :string),
                   schema)
    ensure
      GLib.free(c_abi_schema)
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

  def test_read_only
    open_connection do |connection|
      message =
        "[adbc][connection][set-option]" +
        "[ADBC_STATUS_NOT_IMPLEMENTED (2)][0] " +
        "[SQLite] Unknown connection option adbc.connection.readonly=false"
      assert_raise(ADBC::Error::NotImplemented.new(message)) do
        connection.read_only = false
      end
    end
  end

  def test_isolation_level
    open_connection do |connection|
      message =
        "[adbc][connection][set-option]" +
        "[ADBC_STATUS_NOT_IMPLEMENTED (2)][0] " +
        "[SQLite] Unknown connection option " +
        "adbc.connection.transaction.isolation_level=" +
        "adbc.connection.transaction.isolation.linearizable"
      assert_raise(ADBC::Error::NotImplemented.new(message)) do
        connection.isolation_level = :linearizable
      end
    end
  end

  def test_commit
    open_connection do |connection|
      execute_sql(connection,
                  "CREATE TABLE data (number int, string text)",
                  need_result: false)
      execute_sql(connection,
                  "INSERT INTO data VALUES (1, 'hello')",
                  need_result: false)
    end

    open_connection do |connection|
      connection.auto_commit = false
      execute_sql(connection,
                  "INSERT INTO data VALUES (2, 'world')",
                  need_result: false)
      open_connection do |other_connection|
        execute_sql(other_connection, "SELECT * FROM data") do |table,|
          expected = {
            number: Arrow::Int64Array.new([1]),
            string: Arrow::StringArray.new(["hello"]),
          }
          assert_equal(Arrow::Table.new(expected),
                       table)
        end
      end
      connection.commit
      open_connection do |other_connection|
        execute_sql(other_connection, "SELECT * FROM data") do |table,|
          expected = {
            number: Arrow::Int64Array.new([1, 2]),
            string: Arrow::StringArray.new(["hello", "world"]),
          }
          assert_equal(Arrow::Table.new(expected),
                       table)
        end
      end
    end
  end

  def test_rollback
    open_connection do |connection|
      execute_sql(connection,
                  "CREATE TABLE data (number int, string text)",
                  need_result: false)
      execute_sql(connection,
                  "INSERT INTO data VALUES (1, 'hello')",
                  need_result: false)
    end

    open_connection do |connection|
      connection.auto_commit = false
      execute_sql(connection,
                  "INSERT INTO data VALUES (2, 'world')",
                  need_result: false)
      open_connection do |other_connection|
        execute_sql(other_connection, "SELECT * FROM data") do |table,|
          expected = {
            number: Arrow::Int64Array.new([1]),
            string: Arrow::StringArray.new(["hello"]),
          }
          assert_equal(Arrow::Table.new(expected),
                       table)
        end
      end
      connection.rollback
      open_connection do |other_connection|
        execute_sql(other_connection, "SELECT * FROM data") do |table,|
          expected = {
            number: Arrow::Int64Array.new([1]),
            string: Arrow::StringArray.new(["hello"]),
          }
          assert_equal(Arrow::Table.new(expected),
                       table)
        end
      end
    end
  end
end
