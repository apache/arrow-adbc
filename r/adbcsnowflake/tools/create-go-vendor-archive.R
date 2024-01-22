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

go_bin <- Sys.getenv("GO_BIN", unname(Sys.which("go")))

withr::with_dir("src/go/adbc", {
  system(paste(shQuote(go_bin), "mod vendor -v"))

  # go mod vendor for arrow/v14 doesn't include some files needed for go build
  tmp_zip <- tempfile()
  tmp_extract <- tempfile()
  local({
    on.exit({
      unlink(tmp_extract, recursive = TRUE)
      unlink(tmp_zip)
    })

    curl::curl_download(
      "https://github.com/apache/arrow/archive/refs/tags/go/v14.0.2.zip",
      tmp_zip
    )

    unzip(tmp_zip, exdir = tmp_extract)

    src_go_arrow_cdata_arrow_dir <- file.path(
      tmp_extract,
      "arrow-go-v14.0.2/go/arrow/cdata/arrow"
    )

    dst_go_arrow_cdata_dir <- "vendor/github.com/apache/arrow/go/v14/arrow/cdata/"
    stopifnot(file.copy(src_go_arrow_cdata_arrow_dir, dst_go_arrow_cdata_dir, recursive = TRUE))
  })

  # github.com/zeebo/xxh3/Makefile does not end in LF, giving a check NOTE
  write("\n", "vendor/github.com/zeebo/xxh3/Makefile", append = TRUE)

  # pragmas in this file give a NOTE
  f <- "vendor/golang.org/x/sys/cpu/cpu_gccgo_x86.c"
  content <- paste0(readLines(f), collapse = "\n")
  content <- gsub("#pragma", "// (disabled for CRAN) #pragma", content)
  writeLines(content, f)
})

# Create .zip
dst_zip <- file.path(getwd(), "tools/src-go-adbc-vendor.zip")
unlink(dst_zip)
withr::with_dir("src/go/adbc/vendor", {
  zip(dst_zip, list.files(recursive = TRUE))
})

# Create checksum
dst_checksum <- paste0(dst_zip, ".sha512")
withr::with_dir("tools", {

  system(
    paste(
      "shasum --algorithm 512",
      basename(dst_zip),
      ">",
      basename(dst_checksum)
    )
  )
})
