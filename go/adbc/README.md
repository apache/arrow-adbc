<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# adbc

The adbc package provides:

- Go Interfaces to [Arrow Database Connectivity](https://arrow.apache.org/adbc) (ADBC) for building ADBC drivers
- An ADBC Driver Manager package (`drivermgr`) for loading and using ADBC Drivers
- Scaffolding for writing new drivers in Go (`pkg`)

See the [ADBC Website](https://arrow.apache.org/adbc/current/index.html) for more information on ADBC.

## Getting Started

To use ADBC with your database, you will need:

1. An ADBC driver for your database
2. The ADBC `drivermgr` package to load and use the driver

Below we show how to use the `drivermgr` package to load and use the SQLite ADBC Driver.
Not shown is how to make the SQLite ADBC driver available to the `drivermgr`.

### Installation

```sh
go get github.com/apache/arrow-adbc/go/adbc/drivermgr
```

### Creating a Connection

```go
import (
    "context"
    "github.com/apache/arrow-adbc/go/adbc"
    "github.com/apache/arrow-adbc/go/adbc/drivermgr"
)

ctx := context.Background()

// Create driver and database
var drv drivermgr.Driver
db, err := drv.NewDatabase(map[string]string{
    "driver": "adbc_driver_sqlite",
})
if err != nil {
    return err
}
defer db.Close()

// Open connection
conn, err := db.Open(ctx)
if err != nil {
    return err
}
defer conn.Close()
```

In application code, both the database and connection must be closed after usage or
memory may leak. Use ``defer`` statements to ensure proper cleanup.

### Creating a Statement

```go
stmt, err := conn.NewStatement()
if err != nil {
    return err
}
defer stmt.Close()
```

In application code, the statement must be closed after usage or memory
may leak. Use ``defer`` statements to ensure proper cleanup.

### Executing a Query

We can execute a query and get the results as Arrow data:

```go
err = stmt.SetSqlQuery("SELECT 1, 2.0, 'Hello, world!'")
if err != nil {
    return err
}

reader, _, err := stmt.ExecuteQuery(ctx)
if err != nil {
    return err
}
defer reader.Release()

for reader.Next() {
    record := reader.Record()
    // Access columns by index
    col0 := record.Column(0) // int64 array
    col1 := record.Column(1) // float64 array
    col2 := record.Column(2) // string array

    // Process the data...
    for i := 0; i < int(record.NumRows()); i++ {
        // Access individual values
        fmt.Printf("Row %d: %v, %v, %v\n", i,
            col0.ValueStr(i), col1.ValueStr(i), col2.ValueStr(i))
    }
}
```

### Parameterized Queries

We can bind parameters in our queries using Arrow records:

```go
import (
    "strings"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

// Create parameter schema and data
paramSchema := arrow.NewSchema([]arrow.Field{
    {Name: "param1", Type: arrow.PrimitiveTypes.Int64},
}, nil)

params, _, err := array.RecordFromJSON(memory.DefaultAllocator, paramSchema,
    strings.NewReader(`[{"param1": 41}]`))
if err != nil {
    return err
}
defer params.Release()

// Set query and bind parameters
err = stmt.SetSqlQuery("SELECT ? + 1 AS the_answer")
if err != nil {
    return err
}

err = stmt.Prepare(ctx)
if err != nil {
    return err
}

err = stmt.Bind(ctx, params)
if err != nil {
    return err
}

reader, _, err := stmt.ExecuteQuery(ctx)
if err != nil {
    return err
}
defer reader.Release()
```

### Ingesting Bulk Data

The Go ADBC APIs offer methods for bulk data ingestion. We can insert Arrow data
into a new database table:

```go
// Create Arrow table data
schema := arrow.NewSchema([]arrow.Field{
    {Name: "ints", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
    {Name: "strs", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

table, _, err := array.RecordFromJSON(memory.DefaultAllocator, schema,
    strings.NewReader(`[{"ints": 1, "strs": "a"}, {"ints": 2, "strs": null}]`))
if err != nil {
    return err
}
defer table.Release()

reader, err := array.NewRecordReader(schema, []arrow.Record{table})
if err != nil {
    return err
}
defer reader.Release()

// Ingest the data
count, err := adbc.IngestStream(ctx, conn, reader, "sample",
    adbc.OptionValueIngestModeCreateAppend, adbc.IngestStreamOptions{})
if err != nil {
    return err
}
fmt.Printf("Ingested %d rows\n", count)

// Verify the data
stmt2, err := conn.NewStatement()
if err != nil {
    return err
}
defer stmt2.Close()

err = stmt2.SetSqlQuery("SELECT COUNT(DISTINCT ints) FROM sample")
if err != nil {
    return err
}

reader2, _, err := stmt2.ExecuteQuery(ctx)
if err != nil {
    return err
}
defer reader2.Release()
```

### Getting Database/Driver Metadata

We can get information about the driver and the database:

```go
// Get driver info
infoReader, err := conn.GetInfo(ctx, []adbc.InfoCode{
    adbc.InfoVendorName,
    adbc.InfoDriverName,
})
if err != nil {
    return err
}
defer infoReader.Release()

// Process the info results...
for infoReader.Next() {
    record := infoReader.Record()
    // Extract vendor name, driver name, etc. from the record
}
```

We can also query for tables and columns in the database:

```go
objectsReader, err := conn.GetObjects(ctx, adbc.ObjectDepthAll, nil, nil, nil, nil, nil)
if err != nil {
    return err
}
defer objectsReader.Release()

// Process the objects results to get catalog/schema/table information
for objectsReader.Next() {
    record := objectsReader.Record()
    // Navigate the nested structure for catalogs, schemas, tables, columns
}
```

We can get the Arrow schema of a table:

```go
tableSchema, err := conn.GetTableSchema(ctx, nil, nil, "sample")
if err != nil {
    return err
}

// tableSchema is an *arrow.Schema
fmt.Printf("Table schema: %s\n", tableSchema.String())
```
