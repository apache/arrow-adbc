<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Red ADBC

[![RubyGems: red-adbc](https://img.shields.io/gem/v/red-adbc?style=flat-square)](https://rubygems.org/gems/red-adbc)

Red ADBC is the Ruby bindings of ADBC GLib.

## How to install

If you want to install Red ADBC by Bundler, you can add the following
to your `Gemfile`:

```ruby
plugin "rubygems-requirements-system"

gem "red-adbc"
```

If you want to install Red ADBC by RubyGems, you can use the following
command line:

```console
$ gem install rubygems-requirements-system
$ gem install red-adbc
```

## How to use

```ruby
require "adbc"

ADBC::Database.open(driver: "adbc_driver_sqlite",
                    uri: ":memory:") do |database|
  database.connect do |connection|
    puts(connection.query("SELECT 1"))
  end
end
```
