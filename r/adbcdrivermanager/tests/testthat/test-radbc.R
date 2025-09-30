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
    adbc_connection_get_info(con, double()),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_get_info(con, NULL),
    "NOT_IMPLEMENTED"
  )

  # With defaults of NULL/OL
  expect_error(
    adbc_connection_get_objects(con),
    "NOT_IMPLEMENTED"
  )

  # With explicit args
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

  expect_error(
    adbc_connection_commit(con),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_rollback(con),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_cancel(con),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_get_statistic_names(con),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_connection_get_statistics(con, NULL, NULL, "table name"),
    "NOT_IMPLEMENTED"
  )

  expect_identical(
    adbc_connection_quote_identifier(con, 'some"identifier'),
    '"some""identifier"'
  )

  expect_identical(
    adbc_connection_quote_string(con, "some'value"),
    "'some''value'"
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

  expect_error(
    adbc_statement_execute_schema(stmt),
    "NOT_IMPLEMENTED"
  )

  expect_error(
    adbc_statement_cancel(stmt),
    "NOT_IMPLEMENTED"
  )
})

test_that("invalid external pointer inputs generate errors", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_database_init(list(driver_init_func = character(), load_flags = 0L)),
    "Expected external pointer with class 'adbc_driver_init_func'"
  )

  expect_error(
    adbc_statement_set_sql_query(con, "some query"),
    "Expected external pointer with class 'adbc_statement'"
  )

  # (makes a NULL xptr)
  stmt2 <- unserialize(serialize(stmt, NULL))
  expect_error(
    adbc_statement_set_sql_query(stmt2, "some query"),
    "Can't convert external pointer to NULL to T*"
  )
})

test_that("invalid integer inputs generate errors", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_objects(con, depth = "abc"),
    "Expected integer(1) or double(1)",
    fixed = TRUE
  )

  expect_error(
    adbc_connection_get_objects(con, depth = 1:5),
    "Expected integer(1) or double(1)",
    fixed = TRUE
  )

  expect_error(
    adbc_connection_get_objects(con, structure(1L, class = "non-empty")),
    "Can't convert classed object"
  )

  expect_error(
    adbc_connection_get_objects(con, NA_real_),
    "Can't convert non-finite"
  )
})

test_that("invalid int list inputs generate errors", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_info(con, character()),
    "Expected integer"
  )

  expect_error(
    adbc_connection_get_info(con, structure(integer(), class = "non-empty")),
    "Can't convert classed object"
  )

  expect_error(
    adbc_connection_get_info(con, NA_real_),
    "Can't convert non-finite element"
  )
})

test_that("invalid const char* list inputs generate errors", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_objects(
      con,
      table_type = integer()
    ),
    "Expected character"
  )

  expect_error(
    adbc_connection_get_objects(
      con,
      table_type = NA_character_
    ),
    "Can't convert NA_character_ element"
  )

  expect_error(
    adbc_connection_get_objects(
      con,
      table_type = structure("abc", class = "non-empty")
    ),
    "Can't convert classed object"
  )
})

test_that("invalid const char* inputs generate errors", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_statement_set_sql_query(stmt, NULL),
    "Expected character"
  )

  expect_error(
    adbc_statement_set_sql_query(stmt, structure("abc", class = "non-empty")),
    "Can't convert classed object to const char"
  )

  expect_error(
    adbc_statement_set_sql_query(stmt, NA_character_),
    "Can't convert NA_character_"
  )
})

test_that("invalid bool inputs generate errors", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_statistics(con, NULL, NULL, "table name", character()),
    "Expected integer(1) or double(1)",
    fixed = TRUE
  )

  expect_error(
    adbc_connection_get_statistics(con, NULL, NULL, "table name", NA),
    "Can't convert NA to bool"
  )

  expect_error(
    adbc_connection_get_statistics(
      con, NULL, NULL, "table name",
      structure(TRUE, class = "non-empty")
    ),
    "Can't convert classed object to bool"
  )
})
