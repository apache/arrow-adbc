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

# Download and extract sqlite3
rm -rf sqlite3
curl -L https://www.sqlite.org/2022/sqlite-amalgamation-3400100.zip -o sqlite3.zip
unzip sqlite3.zip -d .
mv sqlite-amalgamation* sqlite3
rm -f sqlite3/shell.c sqlite3/sqlite3ext.h sqlite3.zip
