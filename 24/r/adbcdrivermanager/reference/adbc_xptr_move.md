# Low-level pointer details

- `adbc_xptr_move()` allocates a fresh R object and moves all values
  pointed to by `x` into it. The original R object is invalidated by
  zeroing its content. This is useful when returning from a function
  where [lifecycle
  helpers](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/with_adbc.md)
  were used to manage the original object.

- `adbc_xptr_is_valid()` provides a means by which to test for an
  invalidated pointer.

## Usage

``` r
adbc_xptr_move(x, check_child_count = TRUE)

adbc_xptr_is_valid(x)
```

## Arguments

- x:

  An 'adbc_database', 'adbc_connection', 'adbc_statement', or
  'nanoarrow_array_stream'

- check_child_count:

  Ensures that `x` has a zero child count before performing the move.
  This should almost always be `TRUE`.

## Value

- `adbc_xptr_move()`: A freshly-allocated R object identical to `x`

- `adbc_xptr_is_valid()`: Returns FALSE if the ADBC object pointed to by
  `x` has been invalidated.

## Examples

``` r
db <- adbc_database_init(adbc_driver_void())
adbc_xptr_is_valid(db)
#> [1] TRUE
db_new <- adbc_xptr_move(db)
adbc_xptr_is_valid(db)
#> [1] FALSE
adbc_xptr_is_valid(db_new)
#> [1] TRUE
```
