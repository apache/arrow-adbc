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

# Arrow Database Connectivity for Rust

This is a Rust implementation of [Arrow Database Connectivity (ADBC)](https://arrow.apache.org/adbc).

It currently provides:

- An abstract Rust API to be implemented by vendor-specific drivers.
- A driver manager which implements this same API, but dynamically loads
  drivers internally and forwards calls appropriately using the C API.
- A driver exporter that takes an implementation of the abstract API and
  turns it into an object file that implements the C API.
- A dummy driver implementation for testing and documentation purposes.

## Development

To run the integration tests you must:

1. Install [SQLite](https://www.sqlite.org/) and have its dynamic library in path.
1. Build the official ADBC SQLite driver by following the [documentation](../CONTRIBUTING.md).
1. Place the resulting object file into your dynamic loader path or set
   `LD_LIBRARY_PATH/DYLD_LIBRARY_PATH` appropriately.
1. Run `cargo test --all-features --workspace`

## Writing Drivers

To write an ADBC driver in Rust you have to:

1. Create a new library crate with `crate-type = ["lib", "cdylib"]`.
1. Implement the abstract API which consists of the traits `Driver`, `Database`, `Connection` and `Statement`.
1. Export your driver to C with the macro `adbc_core::export_driver!`.

The resulting object file can then be loaded by other languages trough their own driver manager.
