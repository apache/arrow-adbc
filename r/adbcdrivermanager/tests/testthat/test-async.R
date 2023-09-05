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

test_that("async array_stream$get_next() promises/later integration works", {
  skip_if_not_installed("later")
  skip_if_not_installed("promises")

  stream <- nanoarrow::basic_array_stream(list(1:5))
  loop <- later::create_loop()
  async_called <- FALSE

  promise <- adbc_array_stream_get_next_promise(stream, loop = loop)
  expect_true(promises::is.promising(promise))
  promise$then(function(x) {
    async_called <<- TRUE
    expect_identical(nanoarrow::convert_array(x), 1:5)
  })

  later_loop_wait(loop)
  expect_true(async_called)
})
