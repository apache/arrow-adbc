# Connection methods

Connection methods

## Usage

``` r
adbc_connection_get_info(connection, info_codes = NULL)

adbc_connection_get_objects(
  connection,
  depth = 0L,
  catalog = NULL,
  db_schema = NULL,
  table_name = NULL,
  table_type = NULL,
  column_name = NULL
)

adbc_connection_get_table_schema(connection, catalog, db_schema, table_name)

adbc_connection_get_table_types(connection)

adbc_connection_read_partition(connection, serialized_partition)

adbc_connection_commit(connection)

adbc_connection_rollback(connection)

adbc_connection_cancel(connection)

adbc_connection_get_statistic_names(connection)

adbc_connection_get_statistics(
  connection,
  catalog,
  db_schema,
  table_name,
  approximate = FALSE
)

adbc_connection_quote_identifier(connection, value, ...)

adbc_connection_quote_string(connection, value, ...)
```

## Arguments

- connection:

  An
  [adbc_connection](https://arrow.apache.org/adbc/current/r/adbcdrivermanager/reference/adbc_connection_init.md)

- info_codes:

  A list of metadata codes to fetch, or NULL to fetch all. Valid values
  are documented in the adbc.h header.

- depth:

  The level of nesting to display. If 0, display all levels. If 1,
  display only catalogs (i.e., catalog_schemas will be null). If 2,
  display only catalogs and schemas (i.e., db_schema_tables will be
  null). If 3, display only catalogs, schemas, and tables.

- catalog:

  Only show tables in the given catalog. If NULL, do not filter by
  catalog. If an empty string, only show tables without a catalog. May
  be a search pattern.

- db_schema:

  Only show tables in the given database schema. If NULL, do not filter
  by database schema. If an empty string, only show tables without a
  database schema. May be a search pattern.

- table_name:

  Constrain an object or statistics query for a specific table. If NULL,
  do not filter by name. May be a search pattern.

- table_type:

  Only show tables matching one of the given table types. If NULL, show
  tables of any type. Valid table types can be fetched from
  GetTableTypes. Terminate the list with a NULL entry.

- column_name:

  Only show columns with the given name. If NULL, do not filter by name.
  May be a search pattern.

- serialized_partition:

  The partition descriptor.

- approximate:

  If `FALSE`, request exact values of statistics, else allow for
  best-effort, approximate, or cached values. The database may return
  approximate values regardless, as indicated in the result. Requesting
  exact values may be expensive or unsupported.

- value:

  A string or identifier.

- ...:

  Driver-specific options. For the default method, these are named
  values that are converted to strings.

## Value

- `adbc_connection_get_info()`, `adbc_connection_get_objects()`,
  `adbc_connection_get_table_types()`, and
  `adbc_connection_read_partition()` return a
  [nanoarrow_array_stream](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_array_stream.html).

- `adbc_connection_get_table_schema()` returns a
  [nanoarrow_schena](https://arrow.apache.org/nanoarrow/latest/r/reference/as_nanoarrow_schema.html)

- `adbc_connection_commit()` and `adbc_connection_rollback()` return
  `connection`, invisibly.

## Examples

``` r
db <- adbc_database_init(adbc_driver_void())
con <- adbc_connection_init(db)
# (not implemented by the void driver)
try(adbc_connection_get_info(con, 0))
#> Error in adbc_connection_get_info(con, 0) : NOT_IMPLEMENTED: GetInfo
```
