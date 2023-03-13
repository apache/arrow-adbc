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

test_that("adbcpostgresql() works", {
  expect_s3_class(adbcpostgresql(), "adbc_driver")
})

test_that("default options can open a database and execute a query", {
  server <- test_server_start()
  on.exit(test_server_stop(server))

  # Wait a little bit for server to be ready if we just launched a docker
  # process
  if (!is.null(server$process)) {
    Sys.sleep(2)
  }

  db <- adbcdrivermanager::adbc_database_init(
    adbcpostgresql(),
    uri = server$uri
  )
  expect_s3_class(db, "adbcpostgresql_database")

  con <- adbcdrivermanager::adbc_connection_init(db)
  expect_s3_class(con, "adbcpostgresql_connection")

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  expect_s3_class(stmt, "adbcpostgresql_statement")

  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "CREATE TABLE crossfit (exercise TEXT, difficulty_level INTEGER);"
  )

  # Always attempt to clean up
  on.exit(try({
    stmt <- adbcdrivermanager::adbc_statement_init(con)
    adbcdrivermanager::adbc_statement_set_sql_query(
      stmt,
      "DROP TABLE crossfit;"
    )
    adbcdrivermanager::adbc_statement_execute_query(stmt)
  }, silent = TRUE), add = TRUE, after = TRUE)

  # Currently returns failed but does in fact evaluate the query
  try(
    adbcdrivermanager::adbc_statement_execute_query(stmt),
    silent = TRUE
  )
  adbcdrivermanager::adbc_statement_release(stmt)

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "INSERT INTO crossfit values
      ('Push Ups', 3),
      ('Pull Ups', 5),
      ('Push Jerk', 7),
      ('Bar Muscle Up', 10);"
  )
  # Currently returns failed but does in fact evaluate the query
  try(
    adbcdrivermanager::adbc_statement_execute_query(stmt),
    silent = TRUE
  )
  adbcdrivermanager::adbc_statement_release(stmt)

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  adbcdrivermanager::adbc_statement_set_sql_query(
    stmt,
    "SELECT * from crossfit ORDER BY difficulty_level"
  )

  expect_identical(
    as.data.frame(adbcdrivermanager::adbc_statement_execute_query(stmt)),
    data.frame(
      exercise = c("Push Ups", "Pull Ups", "Push Jerk", "Bar Muscle Up"),
      difficulty_level = c(3L, 5L, 7L, 10L),
      stringsAsFactors = FALSE
    )
  )

  adbcdrivermanager::adbc_statement_release(stmt)
  adbcdrivermanager::adbc_connection_release(con)
  adbcdrivermanager::adbc_database_release(db)
})
