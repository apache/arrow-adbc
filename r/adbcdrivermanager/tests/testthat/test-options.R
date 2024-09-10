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

test_that("get/set option can roundtrip string options for database", {
  db <- adbc_database_init(adbc_driver_void())
  expect_error(
    adbc_database_get_option(db, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_database_set_options(db, list("some_key" = "some value"))
  expect_identical(
    adbc_database_get_option(db, "some_key"),
    "some value"
  )
})

test_that("get/set option can roundtrip bytes options for database", {
  db <- adbc_database_init(adbc_driver_void())
  expect_error(
    adbc_database_get_option_bytes(db, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_database_set_options(db, list("some_key" = charToRaw("some value")))
  expect_identical(
    adbc_database_get_option_bytes(db, "some_key"),
    charToRaw("some value")
  )
})

test_that("get/set option can roundtrip integer options for database", {
  db <- adbc_database_init(adbc_driver_void())

  expect_error(
    adbc_database_get_option_int(db, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_database_set_options(db, list("some_key" = 123L))
  expect_identical(
    adbc_database_get_option_int(db, "some_key"),
    123L
  )
})

test_that("get/set option can roundtrip double options for database", {
  db <- adbc_database_init(adbc_driver_void())

  expect_error(
    adbc_database_get_option_double(db, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_database_set_options(db, list("some_key" = 123.4))
  expect_identical(
    adbc_database_get_option_double(db, "some_key"),
    123.4
  )
})


test_that("get/set option can roundtrip string options for connection", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_option(con, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_connection_set_options(con, list("some_key" = "some value"))
  expect_identical(
    adbc_connection_get_option(con, "some_key"),
    "some value"
  )
})

test_that("get/set option can roundtrip bytes options for connection", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_option_bytes(con, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_connection_set_options(con, list("some_key" = charToRaw("some value")))
  expect_identical(
    adbc_connection_get_option_bytes(con, "some_key"),
    charToRaw("some value")
  )
})

test_that("get/set option can roundtrip int options for connection", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_option_int(con, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_connection_set_options(con, list("some_key" = 123L))
  expect_identical(
    adbc_connection_get_option_int(con, "some_key"),
    123L
  )
})

test_that("get/set option can roundtrip double options for connection", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)

  expect_error(
    adbc_connection_get_option_double(con, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_connection_set_options(con, list("some_key" = 123.4))
  expect_identical(
    adbc_connection_get_option_double(con, "some_key"),
    123.4
  )
})

test_that("get/set option can roundtrip string options for statement", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_statement_get_option(stmt, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_statement_set_options(stmt, list("some_key" = "some value"))
  expect_identical(
    adbc_statement_get_option(stmt, "some_key"),
    "some value"
  )
})

test_that("get/set option can roundtrip bytes options for statement", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_statement_get_option_bytes(stmt, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_statement_set_options(stmt, list("some_key" = charToRaw("some value")))
  expect_identical(
    adbc_statement_get_option_bytes(stmt, "some_key"),
    charToRaw("some value")
  )
})

test_that("get/set option can roundtrip int options for statement", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_statement_get_option_int(stmt, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_statement_set_options(stmt, list("some_key" = 123L))
  expect_identical(
    adbc_statement_get_option_int(stmt, "some_key"),
    123L
  )
})

test_that("get/set option can roundtrip double options for statement", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)

  expect_error(
    adbc_statement_get_option_double(stmt, "some_key"),
    class = "adbc_status_not_found"
  )

  adbc_statement_set_options(stmt, list("some_key" = 123.4))
  expect_identical(
    adbc_statement_get_option_double(stmt, "some_key"),
    123.4
  )
})

test_that("void driver errors getting string option of incompatible type", {
  db <- adbc_database_init(adbc_driver_void())
  adbc_database_set_options(db, list("some_key" = "some value"))

  expect_error(
    adbc_database_get_option_int(db, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_database_get_option_double(db, "some_key"),
    class = "adbc_status_not_found"
  )
})

test_that("void driver can get string option of compatible type", {
  db <- adbc_database_init(adbc_driver_void())
  adbc_database_set_options(db, list("some_key" = "some value"))

  expect_identical(
    adbc_database_get_option_bytes(db, "some_key"),
    charToRaw("some value")
  )
})

test_that("void driver errors getting bytes option of incorrect type", {
  db <- adbc_database_init(adbc_driver_void())
  adbc_database_set_options(db, list("some_key" = charToRaw("some value")))

  expect_error(
    adbc_database_get_option(db, "some_key"),
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

test_that("void driver errors getting integer option of incorrect type", {
  db <- adbc_database_init(adbc_driver_void())
  adbc_database_set_options(db, list("some_key" = 123L))

  expect_error(
    adbc_database_get_option(db, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_database_get_option_bytes(db, "some_key"),
    class = "adbc_status_not_found"
  )


})

test_that("void driver can get integer option of compatible type", {
  db <- adbc_database_init(adbc_driver_void())
  adbc_database_set_options(db, list("some_key" = 123L))

  expect_identical(
    adbc_database_get_option_double(db, "some_key"),
    123.0
  )
})


test_that("void driver errors getting double option of incorrect type", {
  db <- adbc_database_init(adbc_driver_void())
  adbc_database_set_options(db, list("some_key" = 123.4))

  expect_error(
    adbc_database_get_option(db, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_database_get_option_bytes(db, "some_key"),
    class = "adbc_status_not_found"
  )

  expect_error(
    adbc_database_get_option_int(db, "some_key"),
    class = "adbc_status_not_found"
  )
})

test_that("key_value_options works", {
  expect_identical(
    key_value_options(NULL),
    structure(setNames(list(), character()), class = "adbc_options")
  )

  expect_identical(
    key_value_options(c("key" = "value")),
    structure(list("key" = "value"), class = "adbc_options")
  )

  # NULLs dropped
  expect_identical(
    key_value_options(list("key" = "value", "key2" = NULL)),
    structure(list("key" = "value"), class = "adbc_options")
  )

  # Integers stay integers
  expect_identical(
    key_value_options(list("key" = 1L)),
    structure(list("key" = 1L), class = "adbc_options")
  )

  # Doubles stay doubles
  expect_identical(
    key_value_options(list("key" = 1)),
    structure(list("key" = 1), class = "adbc_options")
  )

  # Raw vectors are supported
  expect_identical(
    key_value_options(list("key" = as.raw(c(0x01, 0x05)))),
    structure(list("key" = as.raw(c(0x01, 0x05))), class = "adbc_options")
  )

  # Logical becomes "true" or "false"
  expect_identical(
    key_value_options(list("key" = TRUE, "key2" = FALSE)),
    structure(list("key" = "true", "key2" = "false"), class = "adbc_options")
  )

  # S3 objects converted to strings
  expect_identical(
    key_value_options(list("key" = as.Date("2000-01-01"))),
    structure(list("key" = "2000-01-01"), class = "adbc_options")
  )

  # Can handle many options
  expect_identical(
    key_value_options(setNames(letters, letters)),
    structure(as.list(setNames(letters, letters)), class = "adbc_options")
  )

  # adbc_options() arguments are appended
  expect_identical(
    key_value_options(
      list(
        "key" = "value",
        key_value_options(list("key2" = "value2")),
        "key3" = "value3"
      )
    ),
    structure(
      list("key" = "value", "key2" = "value2", "key3" = "value3"),
      class = "adbc_options"
    )
  )

  # Errors
  expect_error(
    key_value_options(list("value")),
    "must be named"
  )

  expect_error(
    key_value_options(list(key = environment())),
    "Option of type 'environment' (key: 'key') not supported",
    fixed = TRUE
  )

  expect_error(
    key_value_options(setNames(list("value"), "")),
    "must be named"
  )

  expect_error(
    key_value_options(setNames(list("value"), NA_character_)),
    "must be named"
  )
})
