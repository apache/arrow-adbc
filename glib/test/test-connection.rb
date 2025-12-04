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
  include Helper::Sandbox

  setup def setup_database
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
    connection = ADBC::Connection.new
    begin
      connection.init(@database)
      yield(connection)
    ensure
      connection.release
    end
  end

  sub_test_case("#get_info") do
    def test_all
      c_abi_array_stream = @connection.get_info
      import_array_stream(c_abi_array_stream) do |reader|
        table = reader.read_all
        assert_equal([
                       [ADBC::Info::VENDOR_NAME, "PostgreSQL"],
                       [ADBC::Info::VENDOR_VERSION, "X.Y.Z"],
                       [ADBC::Info::DRIVER_NAME, "ADBC PostgreSQL Driver"],
                       [ADBC::Info::DRIVER_VERSION, "(unknown)"],
                       [ADBC::Info::DRIVER_ARROW_VERSION, "X.Y.Z"],
                       [ADBC::Info::DRIVER_ADBC_VERSION, ADBC::VERSION_1_1_0],
                     ],
                     normalize_info(table.raw_records))
      end
    end

    def test_multiple
      codes = [
        ADBC::Info::VENDOR_NAME,
        ADBC::Info::DRIVER_NAME,
      ]
      c_abi_array_stream = @connection.get_info(codes)
      import_array_stream(c_abi_array_stream) do |reader|
        table = reader.read_all
        assert_equal([
                       [ADBC::Info::VENDOR_NAME, "PostgreSQL"],
                       [ADBC::Info::DRIVER_NAME, "ADBC PostgreSQL Driver"],
                     ],
                     normalize_info(table.raw_records))
      end
    end
  end

  sub_test_case("#objects") do
    setup def setup_schema
      execute_sql(@connection,
                  "CREATE TABLE data (number int, string text)",
                  need_result: false)
      execute_sql(@connection,
                  "INSERT INTO data VALUES (1, 'hello')",
                  need_result: false)
      yield
    end

    def get_objects(*args)
      c_abi_array_stream = @connection.get_objects(*args)
      import_array_stream(c_abi_array_stream) do |reader|
        yield(reader.read_all)
      end
    end

    def test_catalogs_all
      get_objects(:catalogs) do |table|
        assert_equal([
                       ["postgres", nil],
                       [@test_db_name, nil],
                       ["template1", nil],
                       ["template0", nil],
                     ],
                     table.raw_records)
      end
    end

    def test_catalogs_match
      get_objects(:catalogs, @test_db_name) do |table|
        assert_equal([[@test_db_name, nil]],
                     table.raw_records)
      end
    end

    def test_catalogs_not_match
      get_objects(:catalogs, "nonexistent") do |table|
        assert_equal([],
                     table.raw_records)
      end
    end

    def test_db_schemas_all
      get_objects(:db_schemas) do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => nil,
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_db_schemas_match
      get_objects(:db_schemas, nil, nil) do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => nil,
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_db_schemas_not_match
      get_objects(:db_schemas, nil, "nonexistent") do |table|
        assert_equal([
                       ["postgres", []],
                       [@test_db_name, []],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_tables_all
      get_objects(:tables) do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [
                               {
                                 "table_columns" => nil,
                                 "table_constraints" => nil,
                                 "table_name" => "data",
                                 "table_type" => "table",
                               },
                             ],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_tables_match
      get_objects(:tables, nil, nil, "data") do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [
                               {
                                 "table_columns" => nil,
                                 "table_constraints" => nil,
                                 "table_name" => "data",
                                 "table_type" => "table",
                               },
                             ],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_tables_not_match
      get_objects(:tables, nil, nil, "nonexistent") do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_table_types_all
      get_objects(:tables) do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [
                               {
                                 "table_columns" => nil,
                                 "table_constraints" => nil,
                                 "table_name" => "data",
                                 "table_type" => "table",
                               },
                             ],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_table_types_match
      get_objects(:tables, nil, nil, nil, ["table", "view"]) do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [
                               {
                                 "table_columns" => nil,
                                 "table_constraints" => nil,
                                 "table_name" => "data",
                                 "table_type" => "table",
                               },
                             ],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_table_types_not_match
      get_objects(:tables, nil, nil, nil, ["nonexistent"]) do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_column_all
      get_objects(:all) do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [
                               {
                                 "table_columns" => [
                                   {
                                     "column_name" => "number",
                                     "ordinal_position" => 1,
                                     "remarks" => nil,
                                     "xdbc_char_octet_length" => nil,
                                     "xdbc_column_def" => nil,
                                     "xdbc_column_size" => nil,
                                     "xdbc_data_type" => nil,
                                     "xdbc_datetime_sub" => nil,
                                     "xdbc_decimal_digits" => nil,
                                     "xdbc_is_autoincrement" => nil,
                                     "xdbc_is_generatedcolumn" => nil,
                                     "xdbc_is_nullable" => nil,
                                     "xdbc_nullable" => nil,
                                     "xdbc_num_prec_radix" => nil,
                                     "xdbc_scope_catalog" => nil,
                                     "xdbc_scope_schema" => nil,
                                     "xdbc_scope_table" => nil,
                                     "xdbc_sql_data_type" => nil,
                                     "xdbc_type_name" => nil,
                                   },
                                   {
                                     "column_name" => "string",
                                     "ordinal_position" => 2,
                                     "remarks" => nil,
                                     "xdbc_char_octet_length" => nil,
                                     "xdbc_column_def" => nil,
                                     "xdbc_column_size" => nil,
                                     "xdbc_data_type" => nil,
                                     "xdbc_datetime_sub" => nil,
                                     "xdbc_decimal_digits" => nil,
                                     "xdbc_is_autoincrement" => nil,
                                     "xdbc_is_generatedcolumn" => nil,
                                     "xdbc_is_nullable" => nil,
                                     "xdbc_nullable" => nil,
                                     "xdbc_num_prec_radix" => nil,
                                     "xdbc_scope_catalog" => nil,
                                     "xdbc_scope_schema" => nil,
                                     "xdbc_scope_table" => nil,
                                     "xdbc_sql_data_type" => nil,
                                     "xdbc_type_name" => nil,
                                   },
                                 ],
                                 "table_constraints" => [],
                                 "table_name" => "data",
                                 "table_type" => "table",
                               }
                             ],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_column_match
      get_objects(:all, nil, nil, nil, nil, "number") do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [
                               {
                                 "table_columns" => [
                                   {
                                     "column_name" => "number",
                                     "ordinal_position" => 1,
                                     "remarks" => nil,
                                     "xdbc_char_octet_length" => nil,
                                     "xdbc_column_def" => nil,
                                     "xdbc_column_size" => nil,
                                     "xdbc_data_type" => nil,
                                     "xdbc_datetime_sub" => nil,
                                     "xdbc_decimal_digits" => nil,
                                     "xdbc_is_autoincrement" => nil,
                                     "xdbc_is_generatedcolumn" => nil,
                                     "xdbc_is_nullable" => nil,
                                     "xdbc_nullable" => nil,
                                     "xdbc_num_prec_radix" => nil,
                                     "xdbc_scope_catalog" => nil,
                                     "xdbc_scope_schema" => nil,
                                     "xdbc_scope_table" => nil,
                                     "xdbc_sql_data_type" => nil,
                                     "xdbc_type_name" => nil,
                                   },
                                 ],
                                 "table_constraints" => [],
                                 "table_name" => "data",
                                 "table_type" => "table",
                               }
                             ],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
      end
    end

    def test_column_not_match
      get_objects(:all, nil, nil, nil, nil, "nonexistent") do |table|
        assert_equal([
                       ["postgres", []],
                       [
                         @test_db_name,
                         [
                           {
                             "db_schema_name" => "public",
                             "db_schema_tables" => [
                               {
                                 "table_columns" => [],
                                 "table_constraints" => [],
                                 "table_name" => "data",
                                 "table_type" => "table",
                               }
                             ],
                           },
                         ],
                       ],
                       ["template1", []],
                       ["template0", []],
                     ],
                     table.raw_records)
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
      assert_equal(Arrow::Schema.new(number: :int32,
                                     string: :string),
                   schema)
    ensure
      GLib.free(c_abi_schema)
    end
  end

  def test_table_types
    c_abi_array_stream = @connection.table_types
    import_array_stream(c_abi_array_stream) do |reader|
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
  end

  def test_read_only
    open_connection do |connection|
      message =
        "[adbc][connection][set-option]" +
        "[NOT_IMPLEMENTED][0] " +
        "[libpq] Unknown option adbc.connection.readonly"
      assert_raise(ADBC::Error::NotImplemented.new(message)) do
        connection.read_only = false
      end
    end
  end

  def test_isolation_level
    open_connection do |connection|
      connection.isolation_level = :serializable
      execute_sql(connection, "SELECT current_setting('transaction_isolation')") do |table,|
        assert_equal("serializable", table["current_setting"][0])
      end
    end
  end

  sub_test_case("#get_statistics") do
    def test_schema
      run_sql("CREATE TABLE public.data1 (number int)")
      run_sql("INSERT INTO public.data1 VALUES (1), (NULL), (2)")
      run_sql("CREATE TABLE public.data2 (name text)")
      run_sql("INSERT INTO public.data2 VALUES ('hello'), (NULL)")
      run_sql("ANALYZE")
      c_abi_array_stream = @connection.get_statistics(nil, "public", nil, true)
      import_array_stream(c_abi_array_stream) do |reader|
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
    end

    def test_schema_table
      run_sql("CREATE TABLE public.data1 (number int)")
      run_sql("INSERT INTO public.data1 VALUES (1), (NULL), (2)")
      run_sql("CREATE TABLE public.data2 (name text)")
      run_sql("ANALYZE")
      c_abi_array_stream =
        @connection.get_statistics(nil, "public", "data1", true)
      import_array_stream(c_abi_array_stream) do |reader|
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
                  ],
                },
              ],
            ],
          ],
          normalize_statistics(table.raw_records)
        )
      end
    end
  end

  def test_statistic_names
    c_abi_array_stream = @connection.statistic_names
    import_array_stream(c_abi_array_stream) do |reader|
      table = reader.read_all
      assert_equal([], table.raw_records)
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
            number: Arrow::Int32Array.new([1]),
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
            number: Arrow::Int32Array.new([1, 2]),
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
            number: Arrow::Int32Array.new([1]),
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
            number: Arrow::Int32Array.new([1]),
            string: Arrow::StringArray.new(["hello"]),
          }
          assert_equal(Arrow::Table.new(expected),
                       table)
        end
      end
    end
  end
end
