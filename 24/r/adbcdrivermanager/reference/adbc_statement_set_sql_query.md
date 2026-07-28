# Statement methods

Statement methods

## Usage

``` r
adbc_statement_set_sql_query(statement, query)

adbc_statement_set_substrait_plan(statement, plan)

adbc_statement_prepare(statement)

adbc_statement_get_parameter_schema(statement)

adbc_statement_bind(statement, values, schema = NULL)

adbc_statement_bind_stream(statement, stream, schema = NULL)

adbc_statement_execute_query(
  statement,
  stream = NULL,
  stream_join_parent = FALSE
)

adbc_statement_execute_schema(statement)

adbc_statement_cancel(statement)
```

## Arguments

- statement:

  An
  [adbc_statement](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_statement_init.md)

- query:

  An SQL query as a string

- plan:

  A raw vector representation of a serialized Substrait plan.

- values:

  A
  [nanoarrow_array](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array.html)
  or object that can be coerced to one.

- schema:

  A
  [nanoarrow_schema](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_schema.html)
  or object that can be coerced to one.

- stream:

  A
  [nanoarrow_array_stream](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array_stream.html)
  or object that can be coerced to one.

- stream_join_parent:

  Use `TRUE` to invalidate `statement` and tie its lifecycle to
  `stream`.

## Value

- `adbc_statement_set_sql_query()`,
  `adbc_statement_set_substrait_plan()`, `adbc_statement_prepare()`,
  `adbc_statement_bind()`, `adbc_statement_bind_stream()`, and
  `adbc_statement_execute_query()` return `statement`, invisibly.

- `adbc_statement_get_parameter_schema()` returns a
  [nanoarrow_schema](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_schema.html).

## Examples

``` r
db <- adbc_database_init(adbc_driver_void())
con <- adbc_connection_init(db)
stmt <- adbc_statement_init(con)
# (not implemented by the void driver)
try(adbc_statement_set_sql_query(stmt, "some query"))
#> Error in adbc_statement_set_sql_query(stmt, "some query") : 
#>   NOT_IMPLEMENTED: SetSqlQuery
```
