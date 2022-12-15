#!/usr/bin/env ruby
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

# Clean old releases from Gemfury.

require "gemfury"

client = Gemfury::Client.new(user_api_key: ENV["GEMFURY_API_TOKEN"])

for artifact in client.list
  puts artifact["name"]
  versions = client.versions(artifact["name"])
  versions.sort_by! { |v| v["created_at"] }

  # Keep last two versions
  for version in versions[0...-2]
    client.yank_version(artifact["name"], version["version"])
    puts "Yanked #{artifact['name']} #{version['version']} (created #{version['created_at']})"
  end
  for version in versions.last(2)
    puts "Kept #{artifact['name']} #{version['version']} (created #{version['created_at']})"
  end
end
