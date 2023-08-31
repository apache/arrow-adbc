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

test_that("aync array_stream$get_next", {
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
