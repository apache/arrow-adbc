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

test_that("adbc_error_from_array_stream() errors for invalid streams", {
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  expect_error(
    adbc_error_from_array_stream(stream),
    "must be a valid nanoarrow_array_stream"
  )
})

test_that("adbc_error_from_array_stream() returns NULL for unrelated streams", {
  stream <- nanoarrow::basic_array_stream(list(1:5))
  expect_null(adbc_error_from_array_stream(stream))
})

test_that("error allocator works", {
  err <- adbc_allocate_error()
  expect_s3_class(err, "adbc_error")

  expect_output(expect_identical(print(err), err), "adbc_error")
  expect_output(expect_identical(str(err), err), "adbc_error")
  expect_identical(length(err), 4L)
  expect_identical(names(err), c("message", "vendor_code", "sqlstate", "details"))
  expect_null(err$message)
  expect_identical(err$sqlstate, as.raw(c(0x00, 0x00, 0x00, 0x00, 0x00)))
  expect_identical(err$details, setNames(list(), character()))
})

test_that("stop_for_error() gives a custom error class with extra info", {
  had_error <- FALSE
  tryCatch({
    db <- adbc_database_init(adbc_driver_void())
    adbc_database_get_option(db, "this option does not exist")
  }, adbc_status = function(e) {
    had_error <<- TRUE
    expect_s3_class(e, "adbc_status")
    expect_s3_class(e, "adbc_status_not_found")
    expect_identical(e$error$status, 3L)
    expect_identical(
      e$error$detail[["adbc.driver_base.option_key"]],
      c(charToRaw("this option does not exist"), as.raw(0x00))
    )
  })

  expect_true(had_error)
})

test_that("void driver can report error to ADBC 1.0.0 structs", {
  opts <- options(adbcdrivermanager.use_legacy_error = TRUE)
  on.exit(options(opts))

  had_error <- FALSE
  tryCatch({
    db <- adbc_database_init(adbc_driver_void())
    adbc_database_get_option(db, "this option does not exist")
  }, adbc_status = function(e) {
    had_error <<- TRUE
    expect_s3_class(e, "adbc_status")
    expect_s3_class(e, "adbc_status_not_found")
    expect_identical(e$error$status, 3L)
    expect_identical(e$error$vendor_code, 0L)
    expect_identical(e$error$details, setNames(list(), character()))
  })

  expect_true(had_error)
})
