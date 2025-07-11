# Apache Arrow ADBC Driver for DuckDB

This is an ADBC (Arrow Database Connectivity) driver for DuckDB, built on top of [DuckDB.NET](https://github.com/Giorgi/DuckDB.NET).

## Features

- Full ADBC API compliance
- Efficient data conversion from DuckDB to Arrow format
- Support for all basic DuckDB data types
- Metadata operations (GetObjects, GetTableSchema, etc.)
- Parameterized queries
- Transaction support

## Installation

```bash
dotnet add package Apache.Arrow.Adbc.Drivers.DuckDB
```

## Usage

```csharp
using Apache.Arrow.Adbc.Drivers.DuckDB;

// Create a driver
var driver = new DuckDBDriver();

// Open a database (in-memory)
var database = driver.Open(new Dictionary<string, string>
{
    { DuckDBParameters.DataSource, ":memory:" }
});

// Create a connection
using var connection = database.Connect(null);

// Execute a query
using var statement = connection.CreateStatement();
statement.SqlQuery = "SELECT 1 as id, 'test' as name";

var result = await statement.ExecuteQueryAsync();
await using var stream = result.Stream;

// Read results as Arrow RecordBatches
while (await stream.ReadNextRecordBatchAsync() is RecordBatch batch)
{
    Console.WriteLine($"Got batch with {batch.Length} rows");
}
```

## Connection Parameters

- `adbc.duckdb.data_source` or `uri` - Path to DuckDB file or `:memory:` for in-memory database
- `adbc.duckdb.batch_size` - Number of rows per RecordBatch (default: 1024)
- `adbc.duckdb.use_chunk_conversion` - Use chunk-based conversion (default: true)
- `adbc.duckdb.enable_memory_pooling` - Enable memory pooling (default: true)

## Data Type Mappings

| DuckDB Type | Arrow Type |
|-------------|------------|
| BOOLEAN | Bool |
| TINYINT | Int8 |
| SMALLINT | Int16 |
| INTEGER | Int32 |
| BIGINT | Int64 |
| UTINYINT | UInt8 |
| USMALLINT | UInt16 |
| UINTEGER | UInt32 |
| UBIGINT | UInt64 |
| FLOAT | Float32 |
| DOUBLE | Float64 |
| DECIMAL | Decimal128 |
| VARCHAR | Utf8 |
| BLOB | Binary |
| DATE | Date32 |
| TIME | Time64 |
| TIMESTAMP | Timestamp |
| UUID | FixedSizeBinary(16) |

## Known Limitations

- No support for bulk parameter binding (Bind/BindStream)
- Limited support for complex nested types
- No direct Arrow export (DuckDB's Arrow C API is deprecated)

## Development

The driver converts DuckDB query results to Arrow format on the client side, as DuckDB's native Arrow C API has been deprecated in favor of the Arrow IPC format.