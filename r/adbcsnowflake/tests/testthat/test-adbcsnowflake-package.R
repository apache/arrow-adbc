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

test_that("adbcsnowflake() works", {
  expect_s3_class(adbcsnowflake(), "adbc_driver")
})

test_that("default options can open a database and execute a query", {
  skip_if_not(adbcsnowflake_has_test_db())

  db <- adbcsnowflake_test_db()
  expect_s3_class(db, "adbcsnowflake_database")

  con <- adbcdrivermanager::adbc_connection_init(db)
  expect_s3_class(con, "adbcsnowflake_connection")

  on.exit({
    adbcdrivermanager::adbc_connection_release(con)
    adbcdrivermanager::adbc_database_release(db)
  })

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  expect_s3_class(stmt, "adbcsnowflake_statement")

  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "SELECT 'abc' as ABC"
  )

  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  adbcdrivermanager::adbc_statement_execute_query(stmt, stream)
  result <- as.data.frame(stream)
  expect_identical(result, data.frame(ABC = "abc"))

  adbcdrivermanager::adbc_statement_release(stmt)
})
