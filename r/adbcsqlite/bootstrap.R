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

# If we are building within the repo, copy the latest adbc.h and driver source
# into src/
source_files <- c(
  "adbc.h",
  "c/driver/sqlite/sqlite.cc",
  "c/driver/sqlite/statement_reader.c",
  "c/driver/sqlite/statement_reader.h",
  "c/driver/common/options.h",
  "c/driver/common/utils.c",
  "c/driver/common/utils.h",
  "c/driver/framework/base_connection.h",
  "c/driver/framework/base_database.h",
  "c/driver/framework/base_driver.cc",
  "c/driver/framework/base_driver.h",
  "c/driver/framework/base_statement.h",
  "c/driver/framework/catalog.h",
  "c/driver/framework/objects.h",
  "c/driver/framework/status.h",
  "c/driver/framework/type_fwd.h",
  "c/driver/framework/catalog.cc",
  "c/driver/framework/objects.cc",
  "c/vendor/fmt/include/fmt/args.h",
  "c/vendor/fmt/include/fmt/base.h",
  "c/vendor/fmt/include/fmt/chrono.h",
  "c/vendor/fmt/include/fmt/color.h",
  "c/vendor/fmt/include/fmt/compile.h",
  "c/vendor/fmt/include/fmt/core.h",
  "c/vendor/fmt/include/fmt/format-inl.h",
  "c/vendor/fmt/include/fmt/format.h",
  "c/vendor/fmt/include/fmt/os.h",
  "c/vendor/fmt/include/fmt/ostream.h",
  "c/vendor/fmt/include/fmt/printf.h",
  "c/vendor/fmt/include/fmt/ranges.h",
  "c/vendor/fmt/include/fmt/std.h",
  "c/vendor/fmt/include/fmt/xchar.h",
  "c/vendor/nanoarrow/nanoarrow.h",
  "c/vendor/nanoarrow/nanoarrow.hpp",
  "c/vendor/nanoarrow/nanoarrow.c",
  "c/vendor/sqlite3/sqlite3.h",
  "c/vendor/sqlite3/sqlite3.c"
)
files_to_vendor <- file.path("../..", source_files)

if (all(file.exists(files_to_vendor))) {
  files_dst <- file.path("src", basename(files_to_vendor))

  n_removed <- suppressWarnings(sum(file.remove(files_dst)))
  if (n_removed > 0) {
    cat(sprintf("Removed %d previously vendored files from src/\n", n_removed))
  }

  cat(
    sprintf(
      "Vendoring files from arrow-adbc to src/:\n%s\n",
      paste("-", files_to_vendor, collapse = "\n")
    )
  )

  if (all(file.copy(files_to_vendor, "src"))) {
    file.rename(files_dst, file.path("src", source_files))
    cat("All files successfully copied to src/\n")
  } else {
    stop("Failed to vendor all files")
  }
}
