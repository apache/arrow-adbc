// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! ADBC: Arrow Database Connectivity
//!
//! ADBC is a set of APIs and libraries for [Arrow](https://arrow.apache.org/)-native
//! access to databases. Execute SQL and [Substrait](https://substrait.io/)
//! queries, query database catalogs, and more, all using Arrow data to
//! eliminate unnecessary data copies, speed up access, and make it more
//! convenient to build analytical applications.
//!
//! Read more about ADBC at <https://arrow.apache.org/adbc/>
//!
//! The `core` library currently provides the basic types shared by vendor-specific drivers,
//! the driver manager, and the driver exporter.
//!
//! # Native Rust drivers
//!
//! Native Rust drivers will implement the abstract API consisting of the traits:
//! - [AsyncDriver]
//! - [AsyncDatabase]
//! - [AsyncConnection]
//! - [AsyncStatement]
//!
//! For drivers implemented in Rust, using these will be more efficient and
//! safe, since it avoids the overhead of going through C FFI.

pub mod constants;
pub mod error;
pub mod executor;
pub mod non_blocking;
pub mod options;
pub mod schemas;
pub mod sync;

pub use sync::*;

pub type LoadFlags = u32;

pub const LOAD_FLAG_SEARCH_ENV: LoadFlags = 1 << 1;
pub const LOAD_FLAG_SEARCH_USER: LoadFlags = 1 << 2;
pub const LOAD_FLAG_SEARCH_SYSTEM: LoadFlags = 1 << 3;
pub const LOAD_FLAG_ALLOW_RELATIVE_PATHS: LoadFlags = 1 << 4;
pub const LOAD_FLAG_DEFAULT: LoadFlags = LOAD_FLAG_SEARCH_ENV
    | LOAD_FLAG_SEARCH_USER
    | LOAD_FLAG_SEARCH_SYSTEM
    | LOAD_FLAG_ALLOW_RELATIVE_PATHS;

/// Each data partition is described by an opaque byte array and can be
/// retrieved with [AsyncConnection::read_partition].
pub type Partitions = Vec<Vec<u8>>;

/// A partitioned result set as returned by [AsyncStatement::execute_partitions].
#[derive(Debug, PartialEq, Eq)]
pub struct PartitionedResult {
    /// The result partitions.
    pub partitions: Partitions,
    /// The schema of the result set.
    pub schema: arrow_schema::Schema,
    /// The number of rows affected if known, else -1.
    pub rows_affected: i64,
}
