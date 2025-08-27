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

![logo](https://raw.githubusercontent.com/apache/arrow/refs/heads/main/docs/source/_static/favicon.ico)

[![crates.io](https://img.shields.io/crates/v/adbc_snowflake.svg)](https://crates.io/crates/adbc_snowflake)
[![docs.rs](https://docs.rs/adbc_snowflake/badge.svg)](https://docs.rs/c)

# Snowflake driver for Arrow Database Connectivity (ADBC)

A [Snowflake](https://www.snowflake.com) [ADBC](https://arrow.apache.org/adbc/)
driver, based on the
[ADBC Snowflake Go driver](https://github.com/apache/arrow-adbc/tree/main/go/adbc/driver/snowflake).

## Example

```rust,no_run
use adbc_core::{
    arrow::arrow_array::{cast::AsArray, types::Decimal128Type},
    Connection, Statement
};
use adbc_snowflake::{connection, database, Driver};

# fn main() -> Result<(), Box<dyn std::error::Error>> {

// Load the driver
let mut driver = Driver::try_load()?;

// Construct a database using environment variables
let mut database = database::Builder::from_env()?.build(&mut driver)?;

// Create a connection to the database
let mut connection = connection::Builder::from_env()?.build(&mut database)?;

// Construct a statement to execute a query
let mut statement = connection.new_statement()?;

// Execute a query
statement.set_sql_query("SELECT 21 + 21")?;
let mut reader = statement.execute()?;

// Check the result
let batch = reader.next().expect("a record batch")?;
assert_eq!(
    batch.column(0).as_primitive::<Decimal128Type>().value(0),
    42
);

# Ok(()) }
```

## Crate features

### Linking Go driver

This crate is a wrapper around the Go driver.

There are different methods to load the Go driver:

#### `bundled` (default)

Builds the driver from source and links it statically. This requires a Go
compiler to be available at build time. This is the default behavior.

#### `linked`

Link the driver at build time. This requires the driver library to be available
both at build- and runtime. Set `ADBC_SNOWFLAKE_GO_LIB_DIR` during the build to
add search paths for the linker.

#### Runtime only

It's also possible to build this crate without the driver and only link at
runtime. This requires disabling the `bundled` and `linked` features. Linking
at runtime is also available when the other features are enabled.

### Configuration

The crate provides builders that can be initialized from environment variables.

#### `env` (default)

Adds `from_env` methods to initialize builders from environment variables.

#### `dotenv`: `env` (default)

Loads environment variables from `.env` files in `from_env` methods.
