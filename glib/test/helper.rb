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

require "tmpdir"

require "arrow"

module Helper
  def require_gi_bindings(major, minor, micro)
    return if GLib.check_binding_version?(major, minor, micro)
    message =
      "Require gobject-introspection #{major}.#{minor}.#{micro} or later: " +
      GLib::BINDING_VERSION.join(".")
    omit(message)
  end

  def import_array_stream(c_abi_array_stream)
    begin
      reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
      begin
        yield(reader)
      ensure
        reader.unref
      end
    ensure
      GLib.free(c_abi_array_stream)
    end
  end

  def execute_statement(statement, need_result: true)
    _, c_abi_array_stream, n_rows_affected = statement.execute(need_result)
    if need_result
      import_array_stream(c_abi_array_stream) do |reader|
        table = reader.read_all
        yield(table, n_rows_affected) if block_given?
      end
    else
      yield(n_rows_affected) if block_given?
    end
  end

  def execute_sql(connection, sql, need_result: true, &block)
    statement = ADBC::Statement.new(connection)
    begin
      statement.set_sql_query(sql)
      execute_statement(statement, need_result: need_result, &block)
    ensure
      statement.release
    end
  end
end
