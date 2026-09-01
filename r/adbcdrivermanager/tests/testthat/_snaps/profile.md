# character driver must be one non-missing string

    Code
      adbc_database_init(character())
    Condition
      Error:
      ! When `driver` is a character vector, it must have length 1 and must not be `NA`.

---

    Code
      adbc_database_init(c("one", "two"))
    Condition
      Error:
      ! When `driver` is a character vector, it must have length 1 and must not be `NA`.

---

    Code
      adbc_database_init(NA_character_)
    Condition
      Error:
      ! When `driver` is a character vector, it must have length 1 and must not be `NA`.

