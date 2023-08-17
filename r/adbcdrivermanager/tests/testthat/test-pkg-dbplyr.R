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

test_that("adbc_simulate_dbi() default method works", {
  skip_if_not_installed("dbplyr")

  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  expect_s3_class(adbc_simulate_dbi(con), "DBIConnection")
})

test_that("Basic SQL generation works with an adbc_connection", {
  skip_if_not_installed("dbplyr")

  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  lf <- dbplyr::lazy_frame(
    a = TRUE, b = 1, c = 2, d = "z",
    con = con
  )

  expect_match(
    as.character(dbplyr::sql_render(lf)),
    "^SELECT"
  )
})
