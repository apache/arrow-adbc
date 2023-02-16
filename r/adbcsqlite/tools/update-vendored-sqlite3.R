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

download.file(
  "https://www.sqlite.org/2022/sqlite-amalgamation-3400100.zip",
  "tools/sqlite.zip",
  mode = "wb"
)

unzip("tools/sqlite.zip", exdir = "tools")
src_dir <- list.files("tools", "sqlite-amalgamation-", full.names = TRUE)
stopifnot(length(src_dir) == 1)

suppressWarnings(file.remove(file.path("tools", c("sqlite3.h", "sqlite3.c"))))
file.copy(
  file.path(src_dir, c("sqlite3.h", "sqlite3.c")),
  "tools"
)

unlink(src_dir, recursive = TRUE)
unlink("tools/sqlite.zip")
