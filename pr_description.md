## Summary

This PR adds a DuckDB driver for Apache Arrow ADBC. The driver enables DuckDB.NET to work with Apache Arrow format data through the ADBC interface.

## Implementation

The driver provides full ADBC support including:

- **Connection management** - Opening/closing DuckDB connections
- **Query execution** - Running SQL queries and returning Arrow format results  
- **Metadata operations** - Schema discovery using information_schema
- **Prepared statements** - Statement preparation and reuse
- **Parameter binding** - Bind Arrow arrays/batches as query parameters
- **Data type conversion** - Mapping between DuckDB and Arrow types

## Technical Details

Since DuckDB deprecated their Arrow C API, the driver converts DuckDB query results to Arrow format using the Apache Arrow C# library. Parameter binding works by converting Arrow values to DuckDB parameters for each row.

## Testing

The driver includes comprehensive tests covering all supported functionality.