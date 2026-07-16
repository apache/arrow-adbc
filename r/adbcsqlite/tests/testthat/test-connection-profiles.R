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

adbcsqlite_shared <- function() {
  lib_name <- paste0("adbcsqlite", .Platform$dynlib.ext)
  r_arch <- .Platform$r_arch

  path <- if (nzchar(r_arch)) {
    system.file("libs", r_arch, lib_name, package = "adbcsqlite")
  } else {
    system.file("libs", lib_name, package = "adbcsqlite")
  }

  if (nzchar(path)) {
    return(path)
  }
  system.file("src", lib_name, package = "adbcsqlite")
}

write_sqlite_profile <- function(dir, name) {
  driver_path <- adbcsqlite_shared()
  stopifnot(file.exists(driver_path))

  content <- paste0(
    'profile_version = 1\n',
    'driver = "',
    driver_path,
    '"\n',
    '[Options]\n'
  )
  path <- file.path(dir, paste0(name, ".toml"))
  writeLines(content, path)
  stopifnot(file.exists(path))

  path
}

test_that("can open a sqlite database via a profile from path via env var", {
  skip_if_not(packageVersion("adbcdrivermanager") >= "0.23.0.9000")

  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_sqlite_profile(dir, "my_sqlite")
  dir <- dirname(profile_path)

  withr::with_envvar(
    list(ADBC_PROFILE_PATH = dir),
    db <- adbc_database_init(
      uri = "profile://my_sqlite"
    ),
  )

  con <- adbcdrivermanager::adbc_connection_init(db)
  on.exit({
    adbcdrivermanager::adbc_connection_release(con)
    adbcdrivermanager::adbc_database_release(db)
  })

  stream <- adbcdrivermanager::read_adbc(con, "SELECT 1 AS numbers")

  expect_identical(
    as.data.frame(stream),
    data.frame(numbers = 1, check.names = FALSE)
  )
})
