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

class ArrowConnectionTest < Test::Unit::TestCase
  include Helper
  include Helper::Sandbox

  setup def setup_database
    omit("adbc-arrow-glib isn't built") unless defined?(ADBCArrow)

    @database = ADBC::Database.new
    @database.set_option("driver", "adbc_driver_postgresql")
    @database.set_option("uri", adbc_uri)
    @database.init
    open_connection do |connection|
      @connection = connection
      yield
    end
  end

  def open_connection
    connection = ADBCArrow::Connection.new
    begin
      connection.init(@database)
      yield(connection)
    ensure
      connection.release
    end
  end

  def test_info
    reader = @connection.get_info
    table = reader.read_all
    assert_equal([
                   [ADBC::Info::VENDOR_NAME, "PostgreSQL"],
                   [ADBC::Info::VENDOR_VERSION, "X.Y.Z"],
                   [ADBC::Info::DRIVER_NAME, "ADBC PostgreSQL Driver"],
                   [ADBC::Info::DRIVER_VERSION, "unknown"],
                   [ADBC::Info::DRIVER_ARROW_VERSION, "vX.Y.Z"],
                   [ADBC::Info::DRIVER_ADBC_VERSION, ADBC::VERSION_1_1_0],
                 ],
                 normalize_info(table.raw_records))
  end

  def test_objects
    reader = @connection.get_objects(:catalogs)
    assert_equal([
                   ["postgres", nil],
                   [@test_db_name, nil],
                   ["template1", nil],
                   ["template0", nil],
                 ],
                 reader.read_all.raw_records)
  end

  def test_table_schema
    execute_sql(@connection,
                "CREATE TABLE data (number int, string text)",
                need_result: false)
    execute_sql(@connection,
                "INSERT INTO data VALUES (1, 'hello')",
                need_result: false)
    assert_equal(Arrow::Schema.new(number: :int32,
                                   string: :string),
                 @connection.get_table_schema(nil, nil, "data"))
  end

  def test_table_types
    reader = @connection.table_types
    table = reader.read_all
    fields = [
      Arrow::Field.new("table_type", :string, false),
    ]
    schema = Arrow::Schema.new(fields)
    table_types = Arrow::StringArray.new([
                                           "partitioned_table",
                                           "foreign_table",
                                           "toast_table",
                                           "materialized_view",
                                           "view",
                                           "table",
                                         ])
    assert_equal(Arrow::Table.new(schema, [table_types]),
                 table)
  end

  def test_statistics
      run_sql("CREATE TABLE public.data1 (number int)")
      run_sql("INSERT INTO public.data1 VALUES (1), (NULL), (2)")
      run_sql("CREATE TABLE public.data2 (name text)")
      run_sql("INSERT INTO public.data2 VALUES ('hello'), (NULL)")
      run_sql("ANALYZE")
      reader = @connection.get_statistics(nil, "public", nil, true)
      table = reader.read_all
      assert_equal(
        [
          [
            @test_db_name,
            [
              {
                "db_schema_name" => "public",
                "db_schema_statistics" => [
                  {
                    "table_name" => "data1",
                    "column_name" => nil,
                    "statistic_key" => ADBC::StatisticKey::ROW_COUNT,
                    "statistic_value" => 3.0,
                    "statistic_is_approximate" => true,
                  },
                  {
                    "table_name" => "data1",
                    "column_name" => "number",
                    "statistic_key" => ADBC::StatisticKey::AVERAGE_BYTE_WIDTH,
                    "statistic_value" => 4.0,
                    "statistic_is_approximate" => true,
                  },
                  {
                    "table_name" => "data1",
                    "column_name" => "number",
                    "statistic_key" => ADBC::StatisticKey::DISTINCT_COUNT,
                    "statistic_value" => 2.0,
                    "statistic_is_approximate" => true,
                  },
                  {
                    "table_name" => "data1",
                    "column_name" => "number",
                    "statistic_key" => ADBC::StatisticKey::NULL_COUNT,
                    "statistic_value" => 1.0,
                    "statistic_is_approximate" => true,
                  },
                  {
                    "table_name" => "data2",
                    "column_name" => nil,
                    "statistic_key" => ADBC::StatisticKey::ROW_COUNT,
                    "statistic_value" => 2.0,
                    "statistic_is_approximate" => true,
                  },
                  {
                    "table_name" => "data2",
                    "column_name" => "name",
                    "statistic_key" => ADBC::StatisticKey::AVERAGE_BYTE_WIDTH,
                    "statistic_value" => 6.0,
                    "statistic_is_approximate" => true,
                  },
                  {
                    "table_name" => "data2",
                    "column_name" => "name",
                    "statistic_key" => ADBC::StatisticKey::DISTINCT_COUNT,
                    "statistic_value" => 1.0,
                    "statistic_is_approximate" => true,
                  },
                  {
                    "table_name" => "data2",
                    "column_name" => "name",
                    "statistic_key" => ADBC::StatisticKey::NULL_COUNT,
                    "statistic_value" => 1.0,
                    "statistic_is_approximate" => true,
                  },
                ],
              },
            ],
          ],
        ],
        normalize_statistics(table.raw_records)
      )
  end

  def test_statistic_names
    reader = @connection.statistic_names
    assert_equal([],
                 reader.read_all.raw_records)
  end
end
