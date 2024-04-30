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
  class Loader < GObjectIntrospection::Loader
    class << self
      def load
        super("ADBC", ADBC)
      end
    end

    private
    def post_load(repository, namespace)
      require_libraries
    end

    def require_libraries
      require_relative "connection"
      require_relative "database"
      require_relative "statement"
    end

    def load_function_info_singleton_method(info, klass, method_name)
      if klass.name == "ADBC::IsolationLevel" and method_name == "to_string"
        define_method(info, klass, "to_s")
      else
        super
      end
    end
  end
end

module ADBCArrow
  class Loader < GObjectIntrospection::Loader
    class << self
      def load
        super("ADBCArrow", ADBCArrow)
      end
    end

    private
    def post_load(repository, namespace)
      require_libraries
    end

    def require_libraries
      require_relative "arrow-connection"
      require_relative "arrow-statement"
    end
  end
end
