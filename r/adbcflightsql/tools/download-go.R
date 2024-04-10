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

tmp_dir <- "src/go/tmp"

go_version <- Sys.getenv("R_ADBC_GO_VERSION_DOWNLOAD", "1.21.8")

go_platform <- tolower(Sys.info()[["sysname"]])
if (!(go_platform %in% c("darwin", "linux", "windows"))) {
  stop(sprintf("Go binary not available for os '%s'", go_platform))
}

r_arch <- R.version$arch
if (identical(r_arch, "aarch64")) {
  go_arch <- "arm64"
} else if (identical(r_arch, "x86_64")) {
  go_arch <- "amd64"
} else if (identical(r_arch, "i386")) {
  go_arch <- "386"
} else {
  stop(sprintf("Go binary not available for arch '%s'", r_arch))
}

if (identical(go_platform, "windows")) {
  go_ext <- "zip"
  go_bin <- paste0(tmp_dir, "/go/bin/go.exe")
} else {
  go_ext <- "tar.gz"
  go_bin <- paste0(tmp_dir, "/go/bin/go")
}

archive_filename <- sprintf("go%s.%s-%s.%s",
  go_version,
  go_platform,
  go_arch,
  go_ext
)

archive_url <- sprintf("https://go.dev/dl/%s", archive_filename)
archive_dest <- file.path(tmp_dir, archive_filename)

if (!dir.exists(tmp_dir)) {
  dir.create(tmp_dir)
}

if (!file.exists(archive_dest)) {
  download.file(archive_url, archive_dest, mode = "wb")
}

# This step takes a while and there is no good way to communicate progress.
# We need to make sure that if the user cancels that we delete the partially
# extracted files (or else `go version`` passes but some parts of the standard
# library may be missing)
do_extract <- function() {
  extract_completed <- FALSE
  on.exit({
    if (!extract_completed) {
      unlink(file.path(tmp_dir, "go"), recursive = TRUE)
    }
  })

  if (identical(go_ext, "zip")) {
    unzip(archive_dest, exdir = tmp_dir)
  } else {
    stopifnot(untar(archive_dest, exdir = tmp_dir, verbose = TRUE) == 0L)
  }

  extract_completed <- TRUE
}

if (!file.exists(go_bin)) {
  do_extract()
}
