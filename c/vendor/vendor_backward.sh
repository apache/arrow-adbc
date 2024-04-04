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

# Check for updates: https://github.com/bombela/backward-cpp

echo "This must be done by hand, because we patched backward-cpp"
echo "Search the existing backward.hpp for XXX(lidavidm)"
exit 1

rm -rf backward
wget -O backward.zip https://github.com/bombela/backward-cpp/archive/refs/heads/master.zip
unzip backward.zip -d .
mkdir -p backward
mv backward-cpp-master/backward.cpp backward/
mv backward-cpp-master/backward.hpp backward/
mv backward-cpp-master/LICENSE.txt backward/
rm -rf backward-cpp-master backward.zip
