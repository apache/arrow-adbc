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

require_relative "connection-operations"

module ADBC
  class Connection
    include ConnectionOperations

    def open_statement(&block)
      Statement.open(self, &block)
    end

    alias_method :get_info_raw, :get_info
    def get_info(codes)
      c_abi_array_stream = get_info_raw(codes)
      begin
        reader = Arrow::RecordBatchReader.import(c_abi_array_stream)
        begin
          yield(reader.read_all)
        ensure
          reader.unref
        end
      ensure
        GLib.free(c_abi_array_stream)
      end
    end
  end
end
