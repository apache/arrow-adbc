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

# If we are building within the repo, copy the go sources into the go/
# directory. Technically this copies all go drivers but this is easier
# than remembering the internal dependency structure of the go sources.
files_to_vendor <- list.files(
  "../../go/adbc",
  "\\.(go|mod|txt|sum|h|c|sql)$",
  recursive = TRUE
)

files_to_vendor_src <- file.path("../../go/adbc", files_to_vendor)
files_to_vendor_dst <- file.path("src/go/adbc", files_to_vendor)

# On Windows, file.copy does not handle symlinks. This
# is not a problem for a user install, where this script
# should not even exist, but the below helps development
# on Windows.
dir.create("src/arrow-adbc", showWarnings = FALSE)
file.copy("../../c/include/arrow-adbc/adbc.h", "src/arrow-adbc/adbc.h")

unlink("src/go/adbc", recursive = TRUE)

cat(
  sprintf(
    "Vendoring files from arrow-adbc/go/adbc to src/go/adbc:\n%s\n",
    paste(
      "-", files_to_vendor_src, " -> ", files_to_vendor_dst,
      collapse = "\n"
    )
  )
)

# Recreate the directory structure
dst_dirs <- unique(dirname(files_to_vendor_dst))
for (dst_dir in dst_dirs) {
  if (!dir.exists(dst_dir)) {
    dir.create(dst_dir, recursive = TRUE)
  }
}

# Copy the files
stopifnot(all(file.copy(files_to_vendor_src, files_to_vendor_dst)))
