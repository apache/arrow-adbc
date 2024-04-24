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
  module ConnectionOperations
    def query(sql, &block)
      open_statement do |statement|
        statement.query(sql, &block)
      end
    end

    def ingest(table_name, values, mode: :create)
      open_statment do |statement|
        statement.ingest(table_name, values, mode: mode)
      end
    end

    def info(codes=nil)
      unless codes.nil?
        codes = codes.collect do |code|
          ADBC::Info.try_convert(code)
        end
      end
      get_info(codes) do |table|
        values = {}
        table.raw_records.each do |code, value|
          value = value.values[0] if value.is_a?(Hash)
          code = ADBC::Info.try_convert(code)
          values[code.nick.gsub("-", "_").to_sym] = value
        end
        values
      end
    end

    ADBC::Info.values.each do |value|
      name = value.nick.gsub("-", "_").to_sym
      define_method(name) do
        info([name])[name]
      end
    end
  end
end
