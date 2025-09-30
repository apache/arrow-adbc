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

class DatabaseTest < Test::Unit::TestCase
  def test_init
    database = ADBC::Database.new
    database.set_option("driver", "adbc_driver_sqlite")
    assert do
      database.init
    end
  end

  def test_release
    database = ADBC::Database.new
    assert do
      database.release
    end
  end

  def test_set_load_flags
    database = ADBC::Database.new
    database.load_flags = [
      :search_env,
      :search_user,
      :search_system,
      :allow_relative_paths,
    ]
    database.load_flags = ADBC::LOAD_FLAGS_DEFAULT
  end
end
