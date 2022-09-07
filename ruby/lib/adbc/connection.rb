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
  class Connection
    def query(sql)
      statement = Statement.new(self)
      begin
        statement.set_sql_query(sql)
        _, c_abi_array_stream, n_rows_affected = statement.execute
        begin
          reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
          if block_given?
            yield(reader, n_rows_affected)
          else
            reader.read_all
          end
        ensure
          GLib.free(c_abi_array_stream)
        end
      ensure
        statement.release
      end
    end
  end
end
