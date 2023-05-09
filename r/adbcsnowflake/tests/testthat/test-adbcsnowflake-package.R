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
  test_db_uri <- Sys.getenv("ADBC_SNOWFLAKE_TEST_URI", "")
  skip_if(identical(test_db_uri, ""))

  db <- adbcdrivermanager::adbc_database_init(
    adbcsnowflake(),
    uri = test_db_uri
  )
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
    "use schema snowflake_sample_data.tpch_sf1;"
  )
  adbcdrivermanager::adbc_statement_execute_query(stmt)
  adbcdrivermanager::adbc_statement_release(stmt)

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  expect_s3_class(stmt, "adbcsnowflake_statement")

  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "SELECT * FROM REGION ORDER BY R_REGIONKEY"
  )

  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  adbcdrivermanager::adbc_statement_execute_query(stmt, stream)
  result <- as.data.frame(stream)
  expect_identical(
    result$R_REGIONKEY,
    c(0, 1, 2, 3, 4)
  )

  adbcdrivermanager::adbc_statement_release(stmt)
})
