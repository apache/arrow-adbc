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

digest_openssl <- function(f) {
  con <- file(f, "rb")
  on.exit(close(con))
  as.character(as.character(openssl::sha512(con)))
}

digest_digest <- function(f) {
  digest::digest(f, algo = "sha512", file = TRUE)
}

read_check <- function(f) {
  con <- file(f, "rb")
  on.exit(close(con))
  scan(con, character(1), n = 1, quiet = TRUE)
}

verify <- function() {
  if (requireNamespace("digest", quietly = TRUE)) {
    cat("Using digest::digest() to verify digest\n")
    digest <- digest_digest("tools/src-go-adbc-vendor.zip")
  } else if (requireNamespace("openssl", quietly = TRUE)) {
    cat("Using openssl::sha512() to verify digest\n")
    digest <- digest_openssl("tools/src-go-adbc-vendor.zip")
  } else {
    cat("openssl nor digest package was installed to verify digest\n")
    return(FALSE)
  }

  digest_check <- read_check("tools/src-go-adbc-vendor.zip.sha512")
  result <- identical(digest_check, digest)

  if (isTRUE(result)) {
    result
  } else {
    cat(sprintf("Digest: %s\n", digest))
    cat(sprintf("Check : %s\n", digest_check))
    FALSE
  }
}

result <- try(verify())

if (!isTRUE(result) && !interactive()) {
  q(status = 1)
} else if (!interactive()) {
  q(status = 0)
}
