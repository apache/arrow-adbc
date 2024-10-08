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

test_that("adbcbigquery() works", {
  expect_s3_class(adbcbigquery(), "adbc_driver")
})

test_that("default options can open a database and execute a query", {
  test_db_uri <- Sys.getenv("ADBC_BIGQUERY_TEST_URI", "")
  skip_if(identical(test_db_uri, ""))

  db <- adbcdrivermanager::adbc_database_init(
    adbcbigquery(),
    uri = test_db_uri
  )
  expect_s3_class(db, "adbcbigquery_database")

  con <- adbcdrivermanager::adbc_connection_init(db)
  expect_s3_class(con, "adbcbigquery_connection")

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  expect_s3_class(stmt, "adbcbigquery_statement")

  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "CREATE TABLE crossfit (exercise TEXT, difficulty_level INTEGER)"
  )
  adbcdrivermanager::adbc_statement_execute_query(stmt)
  adbcdrivermanager::adbc_statement_release(stmt)

  # If we get this far, remove the table and disconnect when the test is done
  on.exit({
    stmt <- adbcdrivermanager::adbc_statement_init(con)
    adbcdrivermanager::adbc_statement_set_sql_query(
      stmt,
      "DROP TABLE IF EXISTS crossfit;"
    )
    adbcdrivermanager::adbc_statement_execute_query(stmt)
    adbcdrivermanager::adbc_statement_release(stmt)

    adbcdrivermanager::adbc_connection_release(con)
    adbcdrivermanager::adbc_database_release(db)
  })

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "INSERT INTO crossfit values
      ('Push Ups', 3),
      ('Pull Ups', 5),
      ('Push Jerk', 7),
      ('Bar Muscle Up', 10);"
  )
  adbcdrivermanager::adbc_statement_execute_query(stmt)
  adbcdrivermanager::adbc_statement_release(stmt)

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "SELECT * from crossfit"
  )

  stream <- nanoarrow::nanoarrow_allocate_array_stream()
  adbcdrivermanager::adbc_statement_execute_query(stmt, stream)

  expect_identical(
    as.data.frame(stream),
    data.frame(
      exercise = c("Push Ups", "Pull Ups", "Push Jerk", "Bar Muscle Up"),
      difficulty_level = c(3, 5, 7, 10),
      stringsAsFactors = FALSE
    )
  )

  adbcdrivermanager::adbc_statement_release(stmt)
})
