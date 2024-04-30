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

# If we are building within the repo, copy the latest adbc.h and driver manager
# implementation from the repo. We also run this from configure, so do nothing
# if we aren't sitting within the repo (e.g., installing a source package from a
# tarball).
files_to_vendor <- c(
  "../../adbc.h",
  "../../c/driver_manager/adbc_driver_manager.h",
  "../../c/driver_manager/adbc_driver_manager.cc",
  "../../c/driver/common/driver_base.h"
)

if (all(file.exists(files_to_vendor))) {
  files_dst <- file.path("src", basename(files_to_vendor))

  n_removed <- sum(file.remove(files_dst))
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
    cat("All files successfully copied to src/\n")
  } else {
    stop("Failed to vendor all files")
  }
}
