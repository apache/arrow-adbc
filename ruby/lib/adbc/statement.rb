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

require_relative "statement-openable"
require_relative "statement-operations"

module ADBC
  class Statement
    extend StatementOpenable
    include StatementOperations

    alias_method :parameter_schema_raw, :parameter_schema
    def parameter_schema
      _, c_abi_schema = parameter_schema_raw
      begin
        Arrow::Schema.import(c_abi_schema)
      ensure
        GLib.free(c_abi_schema)
      end
    end

    alias_method :execute_raw, :execute
    def execute(need_result: true)
      _, c_abi_array_stream, n_rows_affected = execute_raw(need_result)
      if need_result
        begin
          reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
          begin
            if block_given?
              yield(reader, n_rows_affected)
            else
              [reader.read_all, n_rows_affected]
            end
          ensure
            reader.unref
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

    alias_method :bind_raw, :bind
    def bind(*args)
      n_args = args.size
      if block_given?
        message = "wrong number of arguments (given #{n_args}, expected 1 with block)"
        raise ArgumentError, message unless n_args == 1
        values = args[0]
        if values.is_a?(Arrow::Table)
          values = Arrow::TableBatchReader.new(values)
        end
        if values.is_a?(Arrow::RecordBatchReader)
          c_abi_array_stream = values.export
          begin
            bind_stream(c_abi_array_stream)
            yield
          ensure
            GLib.free(c_abi_array_stream)
          end
        else
          _, c_abi_array, c_abi_schema = values.export
          begin
            bind_raw(c_abi_array, c_abi_schema)
            yield
          ensure
            begin
              GLib.free(c_abi_array)
            ensure
              GLib.free(c_abi_schema)
            end
          end
        end
      else
        bind_raw(*args)
      end
    end
  end
end
