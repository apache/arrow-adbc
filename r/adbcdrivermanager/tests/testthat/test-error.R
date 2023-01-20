
test_that("error allocator works", {
  err <- adbc_allocate_error()
  expect_s3_class(err, "adbc_error")

  expect_output(expect_identical(print(err), err), "adbc_error")
  expect_output(expect_identical(str(err), err), "adbc_error")
  expect_identical(length(err), 3L)
  expect_identical(names(err), c("message", "vendor_code", "sqlstate"))
  expect_null(err$message)
  expect_identical(err$vendor_code, 0L)
  expect_identical(err$sqlstate, as.raw(c(0x00, 0x00, 0x00, 0x00, 0x00)))
})
