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

test_that("can initialize and release a database", {
  db <- adbc_database_init(adbc_driver_void(), some_key = "some_value")
  expect_s3_class(db, "adbc_database")
  adbc_database_release(db)
  expect_error(adbc_database_release(db), "INVALID_STATE")
})

test_that("can initialize and release a connection", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db, some_key = "some_value")
  expect_s3_class(con, "adbc_connection")
  adbc_connection_release(con)
  expect_error(adbc_connection_release(con), "INVALID_STATE")
})

test_that("connection methods work for the void driver", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_info(con, integer()),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_get_objects(
      con, 0,
      "catalog", "db_schema",
      "table_name", "table_type", "column_name"
    ),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_get_table_schema(
      con,
      "catalog", "db_schema", "table_name"
    ),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_get_table_types(con),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_read_partition(con, raw()),
    "NOT_IMPLEMENTED"
  )

  expect_identical(
    adbc_connection_commit(con),
    con
  )

  expect_identical(
    adbc_connection_rollback(con),
    con
  )
})

test_that("can initialize and release a statement", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con, some_key = "some_value")
  expect_s3_class(stmt, "adbc_statement")
  adbc_statement_release(stmt)
  expect_error(adbc_statement_release(stmt), "INVALID_STATE")
})

test_that("statement methods work for the void driver", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_statement_set_sql_query(stmt, "some query"),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_statement_set_substrait_plan(stmt, charToRaw("some plan")),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_statement_prepare(stmt),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_statement_get_parameter_schema(stmt),
    "NOT_IMPLEMENTED"
  )

  struct_array <- nanoarrow::as_nanoarrow_array(data.frame(x = 1:5))
  expect_error(
    adbc_statement_bind(stmt, struct_array),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_statement_bind_stream(stmt, nanoarrow::nanoarrow_allocate_array_stream()),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_statement_execute_query(stmt),
    "NOT_IMPLEMENTED"
  )
})

test_that("invalid parameter types generate errors", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_database_init(list(driver_init_func = character())),
    "Expected external pointer with class 'adbc_driver_init_func'"
  )

  expect_error(
    adbc_statement_set_sql_query(con, "some query"),
    "Expected external pointer with class 'adbc_statement'"
  )

  expect_error(
    adbc_connection_get_objects(
      con, NULL,
      "catalog", "db_schema",
      "table_name", "table_type", "column_name"
    ),
    "Expected integer(1) or double(1)",
    fixed = TRUE
  )

  expect_error(
    adbc_statement_set_sql_query(stmt, NULL),
    "Expected character(1)",
    fixed = TRUE
  )

  expect_error(
    adbc_statement_set_sql_query(stmt, NA_character_),
    "Can't convert NA_character_"
  )

  # (makes a NULL xptr)
  stmt2 <- unserialize(serialize(stmt, NULL))
  expect_error(
    adbc_statement_set_sql_query(stmt2, "some query"),
    "Can't convert external pointer to NULL to T*"
  )
})
