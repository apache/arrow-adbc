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

dir.create("src/arrow-adbc", showWarnings = FALSE)
file.copy("../../c/include/arrow-adbc/adbc.h", "src/arrow-adbc/adbc.h")

source_files <- list.files("../../c", "\\.(h|c|cc|hpp)$", recursive = TRUE)
source_files <- source_files[!grepl("_test\\.cc", source_files)]
source_files <- source_files[!grepl("^(build|out)/", source_files)]
source_files <- file.path("c", source_files)
src <- file.path("../..", source_files)
dst <- file.path("src", source_files)

unlink("src/c", recursive = TRUE)
for (dir_name in rev(unique(dirname(dst)))) {
  dir.create(dir_name, showWarnings = FALSE, recursive = TRUE)
}

stopifnot(all(file.copy(src, dst)))

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

  if (!dir.exists("src/copy")) {
    dir.create("src/copy")
  }

  if (all(file.copy(files_to_vendor, "src"))) {
    file.rename(files_dst, file.path("src", source_files))
    cat("All files successfully copied to src/\n")
  } else {
    stop("Failed to vendor all files")
  }
}
