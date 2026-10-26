# Get extended error information from an array stream

Get extended error information from an array stream

## Usage

``` r
adbc_error_from_array_stream(stream)
```

## Arguments

- stream:

  A
  [nanoarrow_array_stream](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array_stream.html)

## Value

`NULL` if stream was not created by a driver that supports extended
error information or a list whose first element is the status code and
second element is the `adbc_error` object. The `acbc_error` must not be
accessed if `stream` is explicitly released.

## Examples

``` r
db <- adbc_database_init(adbc_driver_monkey())
con <- adbc_connection_init(db)
stmt <- adbc_statement_init(con, mtcars)
stream <- nanoarrow::nanoarrow_allocate_array_stream()
adbc_statement_execute_query(stmt, stream)
#> [1] -1
adbc_error_from_array_stream(stream)
#> NULL
```
