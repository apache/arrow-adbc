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

# This suite is just a smoke test to make sure connection profiles are hooked
# up. We make a fake driver and a broken connection profile just to test that
# the driver manager is producing the right error message as proof connection
# profiles are hooked up.

adbc_driver_for_profile <- function() {
  driver <- new.env(parent = emptyenv())
  driver$load_flags <- adbc_load_flags()
  class(driver) <- "adbc_driver"
  driver
}

write_profile <- function(dir, name) {
  content <- paste0(
    'profile_version = 1\n',
    'driver = "/nonexistent/driver.so"\n',
    '\n',
    '[Options]\n'
  )
  path <- file.path(dir, paste0(name, ".toml"))
  writeLines(content, path)
  path
}

test_that("can load a profile by absolute path via 'profile' option", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_profile(dir, "myprofile")

  expect_error(
    adbc_database_init(
      adbc_driver_for_profile(),
      profile = profile_path
    ),
    regexp = "nonexistent"
  )
})

test_that("can load a profile by name via additional_profile_search_path_list", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  write_profile(dir, "myprofile")

  expect_error(
    adbc_database_init(
      adbc_driver_for_profile(),
      profile = "myprofile",
      additional_profile_search_path_list = dir
    ),
    regexp = "nonexistent"
  )
})

test_that("can load a profile by name via ADBC_PROFILE_PATH env var", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  write_profile(dir, "myprofile")

  withr::with_envvar(
    list(ADBC_PROFILE_PATH = dir),
    expect_error(
      adbc_database_init(
        adbc_driver_for_profile(),
        profile = "myprofile"
      ),
      regexp = "nonexistent"
    )
  )
})

test_that("can load a profile via profile:// URI in 'uri' option", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_profile(dir, "myprofile")

  expect_error(
    adbc_database_init(
      adbc_driver_for_profile(),
      uri = paste0("profile://", profile_path)
    ),
    regexp = "nonexistent"
  )
})

test_that("can load a profile via profile:// URI in 'driver' option", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_profile(dir, "myprofile")

  expect_error(
    adbc_database_init_default(
      adbc_driver_for_profile(),
      list(driver = paste0("profile://", profile_path)) # Wrap in list() to avoid arg names clashing
    ),
    regexp = "nonexistent"
  )
})

test_that("missing profile returns an error", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  withr::with_envvar(
    list(ADBC_PROFILE_PATH = dir),
    expect_error(
      adbc_database_init_default(
        adbc_driver_for_profile(),
        list(profile = "does_not_exist")
      ),
      # "does_not_exist" is the profile name; keep in sync with C error message format
      regexp = "does_not_exist"
    )
  )
})
