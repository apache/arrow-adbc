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

test_that("void driver init function works", {
  expect_s3_class(adbc_driver_void(), "adbc_driver")
  expect_s3_class(adbc_driver_void()$driver_init_func, "adbc_driver_init_func")
})

test_that("drivers can be loaded by name/entrypoint", {
  shared <- adbcdrivermanager_shared()
  driver <- adbc_driver(shared, "AdbcTestVoidDriverInit")
  expect_s3_class(driver, "adbc_driver")
  expect_identical(driver$name, shared)
})

test_that("drivers are loaded using load_flags", {
  shared <- adbcdrivermanager_shared()

  withr::with_dir(dirname(shared), {
    expect_s3_class(
      adbc_driver("adbcdrivermanager.so", "AdbcTestVoidDriverInit"),
      "adbc_driver"
    )

    # Check that load_flags are used when test-loading the driver
    expect_error(
      adbc_driver(
        "adbcdrivermanager.so",
        load_flags = adbc_load_flags(allow_relative_paths = FALSE)
      ),
      "Driver path is relative and relative paths are not allowed"
    )

    # Check that load_flags are passed to the database
    drv <- adbc_driver("adbcdrivermanager.so", "AdbcTestVoidDriverInit")
    drv$load_flags <- adbc_load_flags(allow_relative_paths = FALSE)
    expect_error(
      adbc_database_init(drv),
      "Driver path is relative and relative paths are not allowed"
    )
  })
})

test_that("drivers can be loaded by manifest path", {
  toml_content <- sprintf("
name = 'Void Driver'
entrypoint = 'AdbcTestVoidDriverInit'

[ADBC]
version = 'v1.1.0'

[Driver]
[Driver.shared]
%s = '%s'

  ", current_arch(), adbcdrivermanager_shared())

  td <- tempfile()
  toml_path <- file.path(td, "void.toml")
  on.exit(unlink(td))
  dir.create(td)
  writeLines(toml_content, toml_path)

  drv <- adbc_driver(toml_path)
  expect_s3_class(adbc_database_init(drv), "adbc_database")
})
