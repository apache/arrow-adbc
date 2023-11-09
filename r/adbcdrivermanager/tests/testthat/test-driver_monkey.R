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

test_that("the monkey driver sees, and the monkey driver does", {
  db <- adbc_database_init(adbc_driver_monkey())
  expect_s3_class(db, "adbc_database_monkey")
  con <- adbc_connection_init(db)
  expect_s3_class(con, "adbc_connection_monkey")

  input <- data.frame(x = 1:10)
  stmt <- adbc_statement_init(con, input)
  expect_s3_class(stmt, "adbc_statement_monkey")
  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  expect_identical(adbc_statement_execute_query(stmt, stream), -1)
  expect_identical(as.data.frame(stream$get_next()), input)
  adbc_statement_release(stmt)

  stmt <- adbc_statement_init(con, input)
  expect_identical(adbc_statement_execute_query(stmt, NULL), -1)
  adbc_statement_release(stmt)
})
