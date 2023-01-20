
test_that("void driver init function works", {
  expect_s3_class(adbc_driver_void(), "adbc_driver")
  expect_s3_class(adbc_driver_void()$driver_init_func, "adbc_driver_init_func")
})
