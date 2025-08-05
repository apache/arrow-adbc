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

The `adbc` package provides Go interface definitions for using Arrow data and databases, also known as [ADBC](https://arrow.apache.org/adbc/).

## Getting Started

To give a brief tour of what you can do with the `adbc` package, we'll show a brief example of using the [SQLite driver](https://arrow.apache.org/adbc/current/driver/sqlite.html).

### Installation

First, assuming you've already created a Go module, add the `drivermgr` package to your `go.mod` by running:

```sh
go get github.com/apache/arrow-adbc/go/adbc/drivermgr
```

### Imports

For imports, all we really need is `github.com/apache/arrow-adbc/go/adbc/drivermgr` to start but we'll import a few more modules now because we'll use them later.

```go
package main

import (
    "context"
    "fmt"
    "strings"

    "github.com/apache/arrow-adbc/go/adbc"
    "github.com/apache/arrow-adbc/go/adbc/drivermgr"
    "github.com/apache/arrow-go/v18/arrow"
    "github.com/apache/arrow-go/v18/arrow/array"
    "github.com/apache/arrow-go/v18/arrow/memory"
)

func GettingStarted() error {
    ctx := context.Background()

```

### Setup

Any program using ADBC will start with creating a `Database`, a `Connection` to that `Database`, and usually one or more `Statement` objects.

First we create a `Database`, providing `adbc_driver_sqlite` as the name of the driver to load:

```go
    var drv drivermgr.Driver
    db, err := drv.NewDatabase(map[string]string{
        "driver": "adbc_driver_sqlite",
    })
    if err != nil {
        return err
    }
    defer db.Close()
```

Using the `Database` instance we created above, we can now create a `Connection` with `Open`:

```go
    conn, err := db.Open(ctx)
    if err != nil {
        return err
    }
    defer conn.Close()
```

Before we can execute any queries, we need to create a `Statement` to manage them:

```go
    stmt, err := conn.NewStatement()
    if err != nil {
        return err
    }
    defer stmt.Close()
```

Now that we have a basic setup, we'll show some of the functionality.

### Executing a Query

We can execute a query and get the results as Arrow data:

```go
    err = stmt.SetSqlQuery("SELECT 1, 2.0, 'Hello, world!'")
    if err != nil {
        return err
    }

    reader, n, err := stmt.ExecuteQuery(ctx)
    if err != nil {
        return err
    }
    defer reader.Release()
    fmt.Println("Rows affected: ", n)

    for reader.Next() {
        record := reader.Record()

        // Extract our three columns
        col0 := record.Column(0)
        col1 := record.Column(1)
        col2 := record.Column(2)

        for i := 0; i < int(record.NumRows()); i++ {
            fmt.Printf("Row %d: %v, %v, %v\n", i,
                col0.ValueStr(i), col1.ValueStr(i), col2.ValueStr(i))
        }
    }
```

This prints:

```text
Row 0: 1, 2, Hello, world!
```

### Parameterized Queries

We can bind Arrow records as parameters in our queries too:

```go
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

    reader2, _, err := stmt.ExecuteQuery(ctx)
    if err != nil {
        return err
    }
    defer reader2.Release()
```

### Ingesting Bulk Data

You may have noticed that it took a number of steps to bind Arrow data to a query.

New in `adbc` 1.7.0, IngestStream is a utility that simplifies the five-step boilerplate of `NewStatement`, `SetOption`, `Bind`, `Execute`, and `Close`.
Note that IngestStream is not part of the [ADBC Standard](https://arrow.apache.org/adbc/current/format/specification.html).

Here we can use it to create a table from Arrow data.
First, let's prepare some data.

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

    reader3, err := array.NewRecordReader(schema, []arrow.Record{table})
    if err != nil {
        return err
    }
    defer reader3.Release()
```

Now, we can use IngestStream:

```go
    // Ingest the data
    count, err := adbc.IngestStream(ctx, conn, reader3, "sample",
        adbc.OptionValueIngestModeCreateAppend, adbc.IngestStreamOptions{})
    if err != nil {
        return err
    }
    fmt.Printf("Ingested %d rows\n", count)
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

We can also query for tables and columns in the database.

Note: `GetObjects` takes an optional set of filters which control which objects are returned. We set them to `nil` here to return all objects.

```go
    objectsReader, err := conn.GetObjects(
        ctx,
        adbc.ObjectDepthAll,
        nil, /* catalog *string */
        nil, /* dbSchema *string */
        nil, /* tableName *string */
        nil, /* columnName *string */
        nil,  /* tableType []string */
    )
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

This prints:

```text
Table schema: schema:
  fields: 2
    - ints: type=int64, nullable
    - strs: type=utf8, nullable
```

Finally, we close our GettingStarted, call it from `func main`, and handle any errors we returned:

```go
    return nil
} // func GettingStarted

func main() {
   err := GettingStarted()

    if err != nil {
        fmt.Printf("Failed with error: %s", err)
        os.Exit(1)
    }
}
```

If you would like to extract all the code above into a `.go` file, you can run,

```sh
awk '/```go/{flag=1;next}/```/{flag=0}flag' *.md > main.go
```
