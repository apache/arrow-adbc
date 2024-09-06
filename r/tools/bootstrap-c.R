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

exclude <- Sys.getenv("ADBC_R_BOOTSTRAP_EXCLUDE", "")

source_files <- list.files("../../c", "\\.(h|c|cc|hpp)$", recursive = TRUE)

# Apply excludes that apply to all drivers
source_files <- source_files[!grepl("_test\\.cc", source_files)]
source_files <- source_files[!grepl("^(build|out)/", source_files)]
# backward C++ causes CRAN warnings and the drivers do not use it
source_files <- source_files[!grepl("^vendor/backward", source_files)]

# Apply excludes passed via environment variable
if (!identical(exclude, "")) {
  source_files <- source_files[!grepl(exclude, source_files)]
}

# Create source and destination paths
source_files <- file.path("c", source_files)
src <- file.path("../..", source_files)
dst <- file.path("src", source_files)

# Clean existing vendor
unlink("src/c", recursive = TRUE)

# Create folder structure for new vendor
for (dir_name in rev(unique(dirname(dst)))) {
  dir.create(dir_name, showWarnings = FALSE, recursive = TRUE)
}

# Copy files
stopifnot(all(file.copy(src, dst)))
