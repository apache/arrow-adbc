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
    c("error_xptr", "return_code", "rows_affected", "result_xptr", "user_data")
  )

  expect_s3_class(task$error_xptr, "adbc_error")
  expect_identical(task$return_code, NA_integer_)
  expect_identical(task$rows_affected, NA_real_)
  expect_identical(task$result_xptr, NULL)

  expect_identical(adbc_async_task_wait_for(task, 0), "not_started")
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
    adbc_async_task_wait_for(adbc_async_task(), -1),
    "duration_ms must be >= 0"
  )
})

test_that("async sleeper test works", {
  sleep_task <- adbc_async_sleep(500)
  expect_identical(adbc_async_task_wait_for(sleep_task, 0), "timeout")
  expect_identical(adbc_async_task_wait_for(sleep_task, 1000), "ready")
  expect_identical(adbc_async_task_wait_for(sleep_task, 0), "ready")
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

test_that("async task can be converted to a promise", {
  skip_if_not_installed("promises")

  expect_output(
    {
      adbc_async_sleep(100) %>%
        promises::as.promise() %>%
        promises::then(~print(sprintf("waited for %s ms", .x)))

      Sys.sleep(0.2)
      later::run_now()
      later::run_now()
    },
    "waited for 100 ms"
  )

  expect_output(
    {
      adbc_async_sleep(100, error_message = "errored after 100 ms") %>%
        promises::as.promise() %>%
        promises::then(
          onFulfilled = ~print(sprintf("waited for %s ms", .x)),
          onRejected = ~print(.x)
        )
      Sys.sleep(0.2)
      later::run_now()
      later::run_now()
    },
    "errored after 100 ms"
  )
})
