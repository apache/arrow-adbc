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
  test_credentials_file <- Sys.getenv("ADBC_BIGQUERY_TEST_CREDENTIALS_FILE", "")
  skip_if(identical(test_credentials_file, ""))

  client <- gargle::gargle_oauth_client_from_json(test_credentials_file)
  token <- gargle::token_fetch(
    scopes = c(
      "https://www.googleapis.com/auth/bigquery"
    ),
    client = client
  )

  db <- adbcdrivermanager::adbc_database_init(
    adbcbigquery(),
    "adbc.bigquery.sql.project_id" = Sys.getenv("ADBC_BIGQUERY_TEST_PROJECT_ID"),
    token = token
  )
  expect_s3_class(db, "adbcbigquery_database")

  con <- adbcdrivermanager::adbc_connection_init(db)
  expect_s3_class(con, "adbcbigquery_connection")

  stmt <- adbcdrivermanager::adbc_statement_init(con)
  expect_s3_class(stmt, "adbcbigquery_statement")
  adbcdrivermanager::adbc_statement_release(stmt)

  result <- as.data.frame(
    adbcdrivermanager::read_adbc(
      con,
      "SELECT zipcode, latitude, longitude
        FROM `bigquery-public-data.utility_us.zipcode_area` LIMIT 10"
    )
  )
  expect_identical(names(result), c("zipcode", "latitude", "longitude"))
  expect_identical(nrow(result), 10L)
})
