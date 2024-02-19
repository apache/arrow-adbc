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

module ADBCArrow
  class Statement
    extend ADBC::StatementOpenable
    include ADBC::StatementOperations

    alias_method :execute_raw, :execute
    def execute(need_result: true)
      _, reader, n_rows_affected = execute_raw(need_result)
      if need_result
        begin
          if block_given?
            yield(reader, n_rows_affected)
          else
            [reader.read_all, n_rows_affected]
          end
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
          bind_stream(values)
          yield
        else
          bind_raw(values)
          yield
        end
      else
        bind_raw(*args)
      end
    end
  end
end
