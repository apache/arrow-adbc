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
  class Database
    class << self
      def open(**options)
        database = new
        need_release = true
        begin
          options.each do |key, value|
            database.set_option(key, value)
          end
          database.init
          if block_given?
            yield(database)
          else
            need_release = false
            database
          end
        ensure
          database.release if need_release
        end
      end
    end

    def connect(**options)
      connection = Connection.new
      need_release = true
      begin
        options.each do |key, value|
          connection.set_option(key, value)
        end
        connection.init(self)
        if block_given?
          yield(connection)
        else
          need_release = false
          connection
        end
      ensure
        connection.release if need_release
      end
    end
  end
end
