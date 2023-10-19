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

test_that("get option methods work on a database for the void driver", {
  db <- adbc_database_init(adbc_driver_void())
  expect_error(
    adbc_database_get_option(db, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_database_set_options(db, list("some_key" = "some value"))
  expect_identical(adbc_database_get_option(db, "some_key"), "some value")

  expect_error(
    adbc_database_get_option_bytes(db, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_database_get_option_int(db, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_database_get_option_double(db, "some_key"),
    class = "adbc_status_not_found"
  )
})

test_that("get option methods work on a connection for the void driver", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_option(con, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_connection_set_options(con, list("some_key" = "some value"))
  expect_identical(adbc_connection_get_option(con, "some_key"), "some value")

  expect_error(
    adbc_connection_get_option_bytes(con, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_connection_get_option_int(con, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_connection_get_option_double(con, "some_key"),
    class = "adbc_status_not_found"
  )
})

test_that("get option methods work on a statment for the void driver", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_statement_get_option(stmt, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_statement_set_options(stmt, list("some_key" = "some value"))
  expect_identical(adbc_statement_get_option(stmt, "some_key"), "some value")

  expect_error(
    adbc_statement_get_option_bytes(stmt, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_statement_get_option_int(stmt, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_statement_get_option_double(stmt, "some_key"),
    class = "adbc_status_not_found"
  )
})
