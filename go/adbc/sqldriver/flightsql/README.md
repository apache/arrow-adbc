# flightsql - A FlightSQL driver for the database/sql package

[![Go Reference](https://pkg.go.dev/badge/github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql.svg)](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql)

Golang database/sql driver for [FlightSQL](https://arrow.apache.org/docs/format/FlightSql.html).

> Arrow Flight SQL is a protocol for interacting with SQL databases using
> the Arrow in-memory format and the Flight RPC framework.

## Technical Details

This package is a thin wrapper over the
[ADBC database/sql driver wrapper](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/sqldriver),
which is itself database/sql wrapper driver for any
[ADBC](https://arrow.apache.org/docs/format/ADBC.html) driver.

This package simply registers the
[FlightSQL ADBC driver](https://pkg.go.dev/github.com/apache/arrow-adbc/go/adbc/driver/flightsql)
with the database/sql package.
Understanding ADBC is not necessary to use this driver.

## Example

```golang
package main

import (
	"database/sql"
	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
)

func main() {
	db, err := sql.Open("flightsql", "uri=grpc://localhost:12345")
	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
}
```
