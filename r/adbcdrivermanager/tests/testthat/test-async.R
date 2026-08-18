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

test_that("async tasks can be created and inspected", {
  task <- adbc_async_task(subclass = "specific_cls")
  expect_s3_class(task, "adbc_async_task")
  expect_s3_class(task, "specific_cls")

  expect_identical(
    names(task),
    c("error_xptr", "return_code", "user_data", "resolve", "reject")
  )

  expect_s3_class(task$error_xptr, "adbc_error")
  expect_identical(task$return_code, NA_integer_)

  expect_identical(adbc_async_task_status(task), "not_started")
})

test_that("async tasks can update R-level user data", {
  task <- adbc_async_task()
  expect_identical(as.list(task$user_data), list())

  user_data <- task$user_data
  user_data$some_field <- "some_value"
  expect_identical(task$user_data$some_field, "some_value")
})

test_that("async task methods error for invalid input", {
  task <- unserialize(serialize(adbc_async_task(), NULL))
  expect_error(
    names(task),
    "Can't convert external pointer to NULL"
  )

  expect_error(
    adbc_async_task_wait_non_cancellable(adbc_async_task(), -1),
    "duration_ms must be >= 0"
  )
})

test_that("async sleeper test works", {
  sleep_task <- adbc_async_sleep(500)
  expect_identical(adbc_async_task_status(sleep_task), "started")
  expect_identical(adbc_async_task_wait_non_cancellable(sleep_task, 1000), "ready")
  expect_identical(adbc_async_task_status(sleep_task), "ready")
  expect_identical(sleep_task$return_code, 0L)
  expect_identical(adbc_async_task_result(sleep_task), 500)
})

test_that("async task waiter works", {
  sleep_task <- adbc_async_sleep(500)
  expect_identical(adbc_async_task_wait(sleep_task), 500)

  erroring_sleep_task <- adbc_async_sleep(500, error_message = "some error")
  expect_error(
    adbc_async_task_wait(erroring_sleep_task),
    "some error",
    class = "adbc_async_sleep_error"
  )
})

test_that("async tasks can set an R callback", {
  skip_if_not_installed("later")

  async_called <- FALSE
  sleep_task <- adbc_async_sleep(200)
  adbc_async_task_set_callback(sleep_task, function(x) { async_called <<- TRUE })
  Sys.sleep(0.4)
  later::run_now()
  expect_true(async_called)

  # Ensure the callback runs even if the task is already finished
  async_called <- FALSE
  sleep_task <- adbc_async_sleep(0)
  Sys.sleep(0.1)
  adbc_async_task_set_callback(sleep_task, function(x) { async_called <<- TRUE })
  Sys.sleep(0.1)
  expect_true(async_called)

  # Ensure this also works on error
  async_called <- FALSE
  sleep_task <- adbc_async_sleep(0, error_message = "some error")
  Sys.sleep(0.1)
  adbc_async_task_set_callback(
    sleep_task,
    resolve = function(x) NULL,
    reject = function(x) { async_called <<- TRUE }
  )
  Sys.sleep(0.1)
  expect_true(async_called)
})

test_that("async task can be converted to a promise", {
  skip_if_not_installed("promises")

  # Enough time for most CI runners to handle this
  max_wait_s <- 5

  # Check successful call
  async_called <- FALSE
  adbc_async_sleep(100) %>%
    promises::as.promise() %>%
    promises::then(
      onFulfilled = function(duration_ms) {
        expect_identical(duration_ms, 100)
        async_called <<- TRUE
      }
    )

  # Only wait for so long before bailing on this test
  for (i in seq_len(max_wait_s * 100)) {
    later::run_now()
    if (async_called) {
      break
    }

    Sys.sleep(max_wait_s / 100)
  }

  expect_true(async_called)

  # Check erroring call
  async_called <- FALSE
  adbc_async_sleep(100, error_message = "errored after 100 ms") %>%
    promises::as.promise() %>%
    promises::then(
      onRejected = function(reason) {
        expect_s3_class(reason, "adbc_async_sleep_error")
        async_called <<- TRUE
      }
    )

  for (i in seq_len(max_wait_s * 100)) {
    later::run_now()
    if (async_called) {
      break
    }
    Sys.sleep(max_wait_s / 100)
  }

  expect_true(async_called)

})
