
test_that("key_value_options works", {
  expect_identical(
    key_value_options(NULL),
    setNames(character(), character())
  )

  expect_identical(
    key_value_options(c("key" = "value")),
    c("key" = "value")
  )

  expect_identical(
    key_value_options(list("key" = "value")),
    c("key" = "value")
  )

  expect_error(
    key_value_options(list("value")),
    "must be named"
  )

  expect_error(
    key_value_options(setNames(list("value"), "")),
    "must be named"
  )
})

test_that("external pointer embedded environment works", {
  db <- adbc_database_init(adbc_driver_void())
  expect_identical(names(db), "driver")
  expect_identical(length(db), 1L)

  db$key <- "value"
  expect_identical(db$key, "value")

  db[["key"]] <- "value2"
  expect_identical(db[["key"]], "value2")
})
