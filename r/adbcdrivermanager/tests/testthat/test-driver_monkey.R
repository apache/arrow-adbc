
test_that("the monkey driver sees, and the monkey driver does", {
  db <- adbc_database_init(adbc_driver_monkey())
  expect_s3_class(db, "adbc_database_monkey")
  con <- adbc_connection_init(db)
  expect_s3_class(con, "adbc_connection_monkey")

  input <- data.frame(x = 1:10)
  stmt <- adbc_statement_init(con, input)
  expect_s3_class(stmt, "adbc_statement_monkey")
  result <- adbc_statement_execute_query(stmt)
  expect_identical(as.data.frame(result$get_next()), input)
})
