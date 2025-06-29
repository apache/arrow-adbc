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

# ADBC Driver for Apache DataFusion

![Vendor: Apache DataFusion](https://img.shields.io/badge/vendor-Apache%20DataFusion-blue?style=flat-square)
![Implementation: Rust](https://img.shields.io/badge/implementation-Rust-violet?style=flat-square)
![Status: Experimental](https://img.shields.io/badge/status-experimental-red?style=flat-square)

[![crates.io: adbc_datafusion](https://img.shields.io/crates/v/adbc_datafusion?style=flat-square)](https://crates.io/crates/adbc_datafusion)

## Example Usage

```
use adbc_core::driver_manager::ManagedDriver;
use adbc_core::options::AdbcVersion;
use adbc_core::{Connection, Database, Driver, Statement};
use arrow_cast::pretty::print_batches;
use arrow_array::RecordBatch;

fn main() {
    let mut driver = ManagedDriver::load_dynamic_from_name(
        "adbc_datafusion",
        Some(b"DataFusionDriverInit"),
        AdbcVersion::V110,
    )
    .unwrap();

    let database = driver.new_database().unwrap();

    let mut connection = database.new_connection().unwrap();

    let mut statement = connection.new_statement().unwrap();
    let _ = statement.set_sql_query("SELECT 'world' AS Hello");

    let batches: Vec<RecordBatch> = statement.execute().unwrap().map(|b| b.unwrap()).collect();

    print_batches(&batches).unwrap();
}
```
