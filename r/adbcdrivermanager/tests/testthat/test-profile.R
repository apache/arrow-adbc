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

# Smoke tests for loading connection profiles through the R driver manager.

write_broken_profile <- function(dir, name) {
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

write_test_driver_manifest <- function(dir, name) {
  driver_path <- adbcdrivermanager_shared()
  content <- sprintf(
    "
manifest_version = 1

[ADBC]
version = 'v1.1.0'

[Driver]
shared = '%s'
entrypoint = 'AdbcTestVoidDriverInit'
",
    driver_path
  )
  path <- file.path(dir, paste0(name, ".toml"))
  writeLines(content, path)
  path
}

test_that("can initialize a database using a profile", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  manifest_path <- write_test_driver_manifest(dir, "test_driver")
  profile_path <- file.path(dir, "test_profile.toml")
  writeLines(
    c(
      "profile_version = 1",
      sprintf("driver = '%s'", manifest_path),
      "",
      "[Options]",
      "profile_option = 'profile value'"
    ),
    profile_path
  )

  db <- adbc_database_init(profile = profile_path)
  on.exit(adbc_database_release(db), add = TRUE)

  expect_identical(
    adbc_database_get_option(db, "profile_option"),
    "profile value"
  )
})

test_that("can load a profile by absolute path via 'profile' option", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_broken_profile(dir, "myprofile")

  expect_error(
    adbc_database_init(
      profile = profile_path
    ),
    regexp = "nonexistent"
  )
})

test_that("can load a profile by name via additional_profile_search_path_list", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  write_broken_profile(dir, "myprofile")

  expect_error(
    adbc_database_init(
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

  write_broken_profile(dir, "myprofile")

  withr::with_envvar(
    list(ADBC_PROFILE_PATH = dir),
    expect_error(
      adbc_database_init(
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

  profile_path <- write_broken_profile(dir, "myprofile")

  expect_error(
    adbc_database_init(
      uri = paste0("profile://", profile_path)
    ),
    regexp = "nonexistent"
  )
})

test_that("can load a profile via profile:// URI in 'driver' argument", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_broken_profile(dir, "myprofile")

  expect_error(
    adbc_database_init(paste0("profile://", profile_path)),
    regexp = "nonexistent"
  )
})

test_that("character driver must be one non-missing string", {
  expect_snapshot(error = TRUE, adbc_database_init(character()))
  expect_snapshot(error = TRUE, adbc_database_init(c("one", "two")))
  expect_snapshot(error = TRUE, adbc_database_init(NA_character_))
})

test_that("missing profile returns an error", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  withr::with_envvar(
    list(ADBC_PROFILE_PATH = dir),
    expect_error(
      adbc_database_init(profile = "does_not_exist"),
      # "does_not_exist" is the profile name; keep in sync with C error message format
      regexp = "does_not_exist"
    )
  )
})
