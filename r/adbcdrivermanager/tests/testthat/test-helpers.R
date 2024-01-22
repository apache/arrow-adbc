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

test_that("default read/write/execute_adbc() performs the correct calls", {
  # We can't properly test this in the driver manager because we don't
  # have a driver that implements all the methods. Instead, we test this
  # in the sqlite driver and just do a basic log driver snapshot here
  expect_snapshot({
    db <- adbc_database_init(adbc_driver_log())
    try(read_adbc(db, "some sql"))
    try(execute_adbc(db, "some sql"))
    try(write_adbc(mtcars, db, "some_table"))
    adbc_database_release(db)
  })

  expect_snapshot({
    db <- adbc_database_init(adbc_driver_log())
    con <- adbc_connection_init(db)
    try(read_adbc(con, "some sql"))
    try(execute_adbc(con, "some sql"))
    try(write_adbc(mtcars, con, "some_table"))
    adbc_connection_release(con)
    adbc_database_release(db)
  })
})


test_that("with_adbc() and local_adbc() release databases", {
  db <- adbc_database_init(adbc_driver_void())
  expect_identical(with_adbc(db, "value"), "value")
  expect_error(
    adbc_database_release(db),
    "INVALID_STATE"
  )

  db <- adbc_database_init(adbc_driver_void())
  local({
    expect_identical(local_adbc(db), db)
  })
  expect_error(
    adbc_database_release(db),
    "INVALID_STATE"
  )
})

test_that("with_adbc() and local_adbc() release connections", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  expect_identical(with_adbc(con, "value"), "value")
  expect_error(
    adbc_connection_release(con),
    "INVALID_STATE"
  )

  con <- adbc_connection_init(db)
  local({
    expect_identical(local_adbc(con), con)
  })
  expect_error(
    adbc_connection_release(con),
    "INVALID_STATE"
  )
})

test_that("with_adbc() and local_adbc() release statements", {
  db <- adbc_database_init(adbc_driver_void())
  con <- adbc_connection_init(db)
  stmt <- adbc_statement_init(con)
  expect_identical(with_adbc(stmt, "value"), "value")
  expect_error(
    adbc_statement_release(stmt),
    "INVALID_STATE"
  )

  stmt <- adbc_statement_init(con)
  local({
    expect_identical(local_adbc(stmt), stmt)
  })
  expect_error(
    adbc_statement_release(stmt),
    "INVALID_STATE"
  )
})

test_that("with_adbc() and local_adbc() release streams", {
  stream <- nanoarrow::basic_array_stream(list(1:5))
  expect_identical(with_adbc(stream, "value"), "value")
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))

  stream <- nanoarrow::basic_array_stream(list(1:5))
  local({
    expect_identical(local_adbc(stream), stream)
  })
  expect_false(nanoarrow::nanoarrow_pointer_is_valid(stream))
})

test_that("joiners work for databases, connections, and statements", {
  expect_snapshot({
    stmt <- local({
      db <- local_adbc(adbc_database_init(adbc_driver_log()))

      con <- local_adbc(adbc_connection_init(db))
      adbc_connection_join(con, db)
      expect_false(adbc_xptr_is_valid(db))

      stmt <- local_adbc(adbc_statement_init(con))
      adbc_statement_join(stmt, con)
      expect_false(adbc_xptr_is_valid(con))

      adbc_xptr_move(stmt)
    })

    adbc_statement_release(stmt)
  })
})

test_that("joiners work with streams", {
  stream <- local({
    db <- local_adbc(adbc_database_init(adbc_driver_monkey()))

    con <- local_adbc(adbc_connection_init(db))
    adbc_connection_join(con, db)
    expect_false(adbc_xptr_is_valid(db))

    stmt <- local_adbc(adbc_statement_init(con, data.frame(x = 1:5)))
    adbc_statement_join(stmt, con)
    expect_false(adbc_xptr_is_valid(con))

    stream <- local_adbc(nanoarrow::nanoarrow_allocate_array_stream())
    adbc_statement_execute_query(stmt, stream, stream_join_parent = TRUE)
    expect_false(adbc_xptr_is_valid(stmt))

    adbc_xptr_move(stream)
  })

  expect_identical(as.data.frame(stream), data.frame(x = 1:5))
  expect_silent(stream$release())
})
