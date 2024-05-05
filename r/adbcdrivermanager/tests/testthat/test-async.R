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

  expect_identical(adbc_async_task_wait(task, 0), "not_started")
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
    adbc_async_task_wait(adbc_async_task(), -1),
    "duration_ms must be >= 0"
  )

  task <- adbc_async_task()
  expect_error(
    task$result_xptr <- NULL,
    "Can't update field"
  )
})

test_that("async sleeper test works", {
  sleep_task <- adbc_async_sleep(500)
  expect_identical(adbc_async_task_wait(sleep_task, 0), "timeout")
  expect_identical(adbc_async_task_wait(sleep_task, 1000), "ready")
  expect_identical(adbc_async_task_wait(sleep_task, 0), "ready")
  expect_identical(sleep_task$return_code, 0L)
})



test_that("async array_stream$get_next() works", {
  stream <- nanoarrow::basic_array_stream(list(1:5))
  async_called <- FALSE
  queue <- adbc_array_stream_get_next_async(stream, function(array) {
    async_called <<- TRUE
    expect_identical(nanoarrow::convert_array(array), 1:5)
  })

  expect_false(async_called)
  expect_identical(adbc_callback_queue_run_pending(queue), 1)
  expect_true(async_called)
})

test_that("async adbc_statement_execute_query() can return a value", {
  db <- adbc_database_init(adbc_driver_monkey())
  con <- adbc_connection_init(db)
  input <- data.frame(x = 1:5)
  stmt <- adbc_statement_init(con, input)

  async_called <- FALSE
  queue <- adbc_statement_execute_query_async(stmt, function(stream) {
    async_called <<- TRUE
    expect_identical(nanoarrow::convert_array_stream(stream), input)
  })

  expect_false(async_called)
  expect_identical(adbc_callback_queue_run_pending(queue), 1)
  expect_true(async_called)
})

test_that("async adbc_statement_execute_query() can return an error", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  async_called <- FALSE
  queue <- adbc_statement_execute_query_async(
    stmt,
    adbc_callback(
      on_success = function(stream) {
        stop("This should not be called")
      },
      on_error = function(status, error) {
        expect_false(status == 0)
        expect_s3_class(error, "adbc_error")
        async_called <<- TRUE
        stop_for_error(status, error)
      }
    )
  )

  expect_false(async_called)
  expect_error(
    adbc_callback_queue_run_pending(queue),
    class = "adbc_status_not_implemented"
  )
  expect_true(async_called)
  expect_identical(adbc_callback_queue_run_pending(queue), 0)
})

test_that("async array_stream$get_next() promises/later integration works", {
  skip_if_not_installed("later")
  skip_if_not_installed("promises")

  stream <- nanoarrow::basic_array_stream(list(1:5))
  loop <- later::create_loop()
  async_called <- FALSE

  promise <- adbc_array_stream_get_next_promise(stream, loop = loop)
  promise$then(function(x) {
    async_called <<- TRUE
    expect_identical(nanoarrow::convert_array(x), 1:5)
  })

  later_loop_wait(loop)
  expect_true(async_called)
})

test_that("async adbc_execute_query() promises/later integration works", {
  skip_if_not_installed("later")
  skip_if_not_installed("promises")

  db <- adbc_database_init(adbc_driver_monkey())
  con <- adbc_connection_init(db)
  input <- data.frame(x = 1:5)
  stmt <- adbc_statement_init(con, input)

  loop <- later::create_loop()
  async_called <- FALSE

  adbc_statement_execute_query_promise(stmt, loop = loop)$then(function(x) {
    async_called <<- TRUE
    expect_identical(nanoarrow::convert_array_stream(x), input)
  })

  later_loop_wait(loop)
  expect_true(async_called)
})

test_that("async adbc_execute_query() promises/later integration can error", {
  skip_if_not_installed("later")
  skip_if_not_installed("promises")

  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  loop <- later::create_loop()
  async_called <- FALSE

  adbc_statement_execute_query_promise(stmt, loop = loop)$then(
    onFulfilled = function(x) stop("should not be called"),
    onRejected = function(x) {
      async_called <<- TRUE
    }
  )

  later_loop_wait(loop)
  expect_true(async_called)
})

test_that("execute_query and get_next promises can be chained", {
  skip_if_not_installed("later")
  skip_if_not_installed("promises")

  db <- adbc_database_init(adbc_driver_monkey())
  con <- adbc_connection_init(db)
  input <- data.frame(x = 1:5)
  stmt <- adbc_statement_init(con, input)

  loop <- later::create_loop()
  async_called <- FALSE

  later::with_loop(loop, {
    adbc_statement_execute_query_promise(stmt)$then(function(x) {
      adbc_array_stream_get_next_promise(x)
    })$then(function(x) {
      async_called <<- TRUE
      expect_identical(nanoarrow::convert_array(x), input)
    })

    later_loop_wait()
    expect_true(async_called)
  })
})
