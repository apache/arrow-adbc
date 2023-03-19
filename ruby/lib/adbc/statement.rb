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

module ADBC
  class Statement
    class << self
      def open(connection)
        statement = new(connection)
        if block_given?
          begin
            yield(statement)
          ensure
            statement.release
          end
        else
          statement
        end
      end
    end

    alias_method :execute_raw, :execute
    def execute(need_result: true)
      _, c_abi_array_stream, n_rows_affected = execute_raw(need_result)
      if need_result
        begin
          reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
          if block_given?
            yield(reader, n_rows_affected)
          else
            [reader.read_all, n_rows_affected]
          end
        ensure
          GLib.free(c_abi_array_stream)
        end
      else
        if block_given?
          yield(n_rows_affected)
        else
          n_rows_affected
        end
      end
    end

    def ingest(table_name, values, mode: :create)
      insert = "INSERT INTO #{table_name} (" # TODO escape
      fields = values.schema.fields
      insert << fields.collect(&:name).join(", ")
      insert << ") VALUES ("
      insert << (["?"] * fields.size).join(", ")
      insert << ")"
      self.sql_query = insert
      self.ingest_target_table = table_name
      self.ingest_mode = mode
      _, c_abi_array, c_abi_schema = values.export
      begin
        bind(c_abi_array, c_abi_schema)
        execute(need_result: false)
      ensure
        begin
          GLib.free(c_abi_array)
        ensure
          GLib.free(c_abi_schema)
        end
      end
    end

    def query(sql, &block)
      self.sql_query = sql
      execute(&block)
    end
  end
end
