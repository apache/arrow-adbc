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

# Profile tests require adbcsqlite to be installed so we have a real driver
# to reference from profile TOML files.
skip_if_not(
  nzchar(system.file(package = "adbcsqlite")),
  "adbcsqlite package not installed"
)

# Return the path to the adbcsqlite shared library so profiles can reference it.
adbcsqlite_shared <- function() {
  shared <- system.file(
    "libs",
    .Platform$r_arch,
    paste0("adbcsqlite", .Platform$dynlib.ext),
    package = "adbcsqlite"
  )
  if (!identical(shared, "")) return(shared)

  system.file(
    "src",
    paste0("adbcsqlite", .Platform$dynlib.ext),
    package = "adbcsqlite"
  )
}

# Build a minimal driver object that carries no pre-loaded init func or name.
# This lets adbc_database_init_default call RAdbcDatabaseNew(NULL, load_flags),
# leaving the driver manager free to load the driver specified in the profile.
adbc_driver_for_profile <- function() {
  driver <- new.env(parent = emptyenv())
  driver$load_flags <- adbc_load_flags()
  class(driver) <- "adbc_driver"
  driver
}

# Write a simple profile TOML that loads the sqlite driver with an in-memory db.
write_profile <- function(dir, name) {
  content <- paste0(
    'profile_version = 1\n',
    'driver = "', adbcsqlite_shared(), '"\n',
    '\n',
    '[Options]\n',
    'uri = ":memory:"\n'
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

  db <- adbc_database_init_default(
    adbc_driver_for_profile(),
    list(profile = profile_path)
  )
  expect_s3_class(db, "adbc_database")

  con <- adbc_connection_init(db)
  expect_s3_class(con, "adbc_connection")
  adbc_connection_release(con)
  adbc_database_release(db)
})

test_that("can load a profile by name via additional_profile_search_path_list", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  write_profile(dir, "myprofile")

  db <- adbc_database_init_default(
    adbc_driver_for_profile(),
    list(
      profile = "myprofile",
      additional_profile_search_path_list = dir
    )
  )
  expect_s3_class(db, "adbc_database")

  con <- adbc_connection_init(db)
  adbc_connection_release(con)
  adbc_database_release(db)
})

test_that("can load a profile by name via ADBC_PROFILE_PATH env var", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  write_profile(dir, "myprofile")

  withr::with_envvar(
    list(ADBC_PROFILE_PATH = dir),
    {
      db <- adbc_database_init_default(
        adbc_driver_for_profile(),
        list(profile = "myprofile")
      )
      expect_s3_class(db, "adbc_database")

      con <- adbc_connection_init(db)
      adbc_connection_release(con)
      adbc_database_release(db)
    }
  )
})

test_that("can load a profile via profile:// URI in 'uri' option", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_profile(dir, "myprofile")

  db <- adbc_database_init_default(
    adbc_driver_for_profile(),
    list(uri = paste0("profile://", profile_path))
  )
  expect_s3_class(db, "adbc_database")

  con <- adbc_connection_init(db)
  adbc_connection_release(con)
  adbc_database_release(db)
})

test_that("can load a profile via profile:// URI in 'driver' option", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  profile_path <- write_profile(dir, "myprofile")

  db <- adbc_database_init_default(
    adbc_driver_for_profile(),
    list(driver = paste0("profile://", profile_path))
  )
  expect_s3_class(db, "adbc_database")

  con <- adbc_connection_init(db)
  adbc_connection_release(con)
  adbc_database_release(db)
})

test_that("missing profile returns an error", {
  dir <- tempfile()
  dir.create(dir)
  on.exit(unlink(dir, recursive = TRUE))

  withr::with_envvar(
    list(ADBC_PROFILE_PATH = dir),
    {
      expect_error(
        adbc_database_init_default(
          adbc_driver_for_profile(),
          list(profile = "does_not_exist")
        )
      )
    }
  )
})
