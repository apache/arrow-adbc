#!/bin/bash
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

# Check for updates: https://fmt.dev/latest/index.html
# XXX: manually update fmt/CMakeLists.txt so add fmt as STATIC
rm -rf fmt
curl -L https://github.com/fmtlib/fmt/archive/refs/heads/master.zip > fmt.zip
unzip fmt.zip -d .
rm fmt.zip
mv fmt-* fmt
rm -rf fmt/doc fmt/test
rm -rf fmt/support/bazel fmt/support/rtd
rm -rf fmt/support/C++.sublime-syntax fmt/support/README fmt/support/Vagrantfile
rm -rf fmt/support/*.gradle fmt/support/*.mk fmt/support/*.py fmt/support/*.xml
rm -rf fmt/.github
