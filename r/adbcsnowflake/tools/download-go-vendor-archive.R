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

uri <- Sys.getenv("R_ADBCSNOWFLAKE_VENDORED_DEPENDENCY_URI", "")

if (identical(uri, "")) {
  # When there is a home for this that is not a local file:// URI,
  # this is where we would set the default.
  stop("R_ADBCSNOWFLAKE_VENDORED_DEPENDENCY_URI is not set")
}

cat(sprintf("Downloading vendored dependency archive from %s\n", uri))
unlink("tools/src-go-adbc-vendor.zip")
local({
  opts <- options(timeout = max(300, getOption("timeout")))
  on.exit(options(opts))
  download.file(uri, "tools/src-go-adbc-vendor.zip", mode = "wb")
})
