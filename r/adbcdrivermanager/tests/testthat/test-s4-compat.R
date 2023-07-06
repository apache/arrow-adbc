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

test_that("S4 slot definitions work with adbc_* S3 object defined in this package", {
  expect_s4_class(
    adbc_test_s4_compat(adbc_driver_void()),
    "AdbcS4CompatTest"
  )

  expect_s4_class(
    adbc_test_s4_compat(adbc_driver_monkey()),
    "AdbcS4CompatTest"
  )

  expect_snapshot({
    expect_s4_class(
      obj <- adbc_test_s4_compat(adbc_driver_log()),
      "AdbcS4CompatTest"
    )
    adbc_statement_release(obj@statement)
    adbc_connection_release(obj@connection)
    adbc_database_release(obj@database)
  })
})
