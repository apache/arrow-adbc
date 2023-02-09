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

//! Rust structs and utilities for using and building Arrow Database Connectivity (ADBC) drivers.
//!
//! ADBC drivers provide an ABI-stable interface for interacting with databases,
//! that:
//!
//!  * Uses the Arrow [C Data interface](https://arrow.apache.org/docs/format/CDataInterface.html)
//!    and [C Stream Interface](https://arrow.apache.org/docs/format/CStreamInterface.html)
//!    for efficient data interchange.
//!  * Supports partitioned result sets for multi-threaded or distributed
//!    applications.
//!  * Support for [Substrait](https://substrait.io/) plans in addition to SQL queries.
//!
//! When implemented for remote databases, [Flight SQL](https://arrow.apache.org/docs/format/FlightSql.html)
//! can be used as the communication protocol. This means data can be in Arrow
//! format through the whole connection, minimizing serialization and deserialization
//! overhead.
//!
//! Read more about ADBC at <https://arrow.apache.org/adbc/>
//!
//! ## Using ADBC drivers
//!
//! The [driver_manager] mod allows loading drivers, either from an initialization
//! function or by dynamically finding such a function in a dynamic library.
//!
//! ```
//! use arrow::datatypes::Int64Type;
//! use arrow::array::as_primitive_array;
//! use arrow::record_batch::RecordBatchReader;
//!
//! use arrow_adbc::driver_manager::AdbcDriver;
//! use arrow_adbc::ADBC_VERSION_1_0_0;
//! use arrow_adbc::interface::StatementApi;
//!
//! # fn main() -> arrow_adbc::driver_manager::Result<()> {
//! let sqlite_driver = AdbcDriver::load("adbc_driver_sqlite", None, ADBC_VERSION_1_0_0)?;
//! let sqlite_database = sqlite_driver.new_database()?.init()?;
//! let sqlite_conn = sqlite_database.new_connection()?.init()?;
//! let mut sqlite_statement = sqlite_conn.new_statement()?;
//!
//! sqlite_statement.set_sql_query("SELECT 1");
//! let mut results: Box<dyn RecordBatchReader> = sqlite_statement.execute()?
//!     .result.expect("Query did not have a result");
//!
//! let batch = results.next().expect("Result did not have at least one batch")?;
//! let result = as_primitive_array::<Int64Type>(batch.column(0));
//!
//! assert_eq!(result.value(0), 1);
//! # Ok(())
//! # }
//! ```
//!
//! ## Implementing ADBC drivers
//!
//! To implement an ADBC driver, use the [implement] module. The macro
//! [adbc_init_func] will generate adapters from the safe Rust traits you implement
//! to the FFI interface recognized by ADBC.
pub mod driver_manager;
pub mod error;
pub mod ffi;
pub mod implement;
pub mod interface;

pub const ADBC_VERSION_1_0_0: i32 = 1000000;

/// Known options that can be set on databases, connections, and statements.
///
/// For use with [crate::interface::DatabaseApi::set_option],
/// [crate::interface::ConnectionApi::set_option],
/// and [crate::interface::StatementApi::set_option].
pub mod options {
    pub const INGEST_OPTION_TARGET_TABLE: &str = "adbc.ingest.target_table";
    pub const ADBC_INGEST_OPTION_MODE: &str = "adbc.ingest.mode";
    pub const ADBC_INGEST_OPTION_MODE_CREATE: &str = "adbc.ingest.mode.create";
    pub const ADBC_INGEST_OPTION_MODE_APPEND: &str = "adbc.ingest.mode.append";

    /// The name of the canonical option for whether autocommit is enabled.
    pub const ADBC_CONNECTION_OPTION_AUTOCOMMIT: &str = "adbc.connection.autocommit";
    /// The name of the canonical option for whether the current connection should
    /// be restricted to being read-only.
    pub const ADBC_CONNECTION_OPTION_READ_ONLY: &str = "adbc.connection.readonly";
    /// The name of the canonical option for setting the isolation level of a
    /// transaction.
    ///
    /// Should only be used in conjunction with autocommit disabled and
    /// AdbcConnectionCommit / AdbcConnectionRollback. If the desired
    /// isolation level is not supported by a driver, it should return an
    /// appropriate error.
    pub const ADBC_CONNECTION_OPTION_ISOLATION_LEVEL: &str =
        "adbc.connection.transaction.isolation_level";
    /// Use database or driver default isolation level
    pub const ADBC_OPTION_ISOLATION_LEVEL_DEFAULT: &str =
        "adbc.connection.transaction.isolation.default";
    /// The lowest isolation level. Dirty reads are allowed, so one transaction
    /// may see not-yet-committed changes made by others.
    pub const ADBC_OPTION_ISOLATION_LEVEL_READ_UNCOMMITTED: &str =
        "adbc.connection.transaction.isolation.read_uncommitted";
    /// Lock-based concurrency control keeps write locks until the
    /// end of the transaction, but read locks are released as soon as a
    /// SELECT is performed. Non-repeatable reads can occur in this
    /// isolation level.
    ///
    /// More simply put, Read Committed is an isolation level that guarantees
    /// that any data read is committed at the moment it is read. It simply
    /// restricts the reader from seeing any intermediate, uncommitted,
    /// 'dirty' reads. It makes no promise whatsoever that if the transaction
    /// re-issues the read, it will find the same data; data is free to change
    /// after it is read.
    pub const ADBC_OPTION_ISOLATION_LEVEL_READ_COMMITTED: &str =
        "adbc.connection.transaction.isolation.read_committed";
    /// Lock-based concurrency control keeps read AND write locks
    /// (acquired on selection data) until the end of the transaction.
    ///
    /// However, range-locks are not managed, so phantom reads can occur.
    /// Write skew is possible at this isolation level in some systems.
    pub const ADBC_OPTION_ISOLATION_LEVEL_REPEATABLE_READ: &str =
        "adbc.connection.transaction.isolation.repeatable_read";
    /// This isolation guarantees that all reads in the transaction
    /// will see a consistent snapshot of the database and the transaction
    /// should only successfully commit if no updates conflict with any
    /// concurrent updates made since that snapshot.
    pub const ADBC_OPTION_ISOLATION_LEVEL_SNAPSHOT: &str =
        "adbc.connection.transaction.isolation.snapshot";
    /// Serializability requires read and write locks to be released
    /// only at the end of the transaction. This includes acquiring range-
    /// locks when a select query uses a ranged WHERE clause to avoid
    /// phantom reads.
    pub const ADBC_OPTION_ISOLATION_LEVEL_SERIALIZABLE: &str =
        "adbc.connection.transaction.isolation.serializable";
    /// The central distinction between serializability and linearizability
    /// is that serializability is a global property; a property of an entire
    /// history of operations and transactions. Linearizability is a local
    /// property; a property of a single operation/transaction.
    ///
    /// Linearizability can be viewed as a special case of strict serializability
    /// where transactions are restricted to consist of a single operation applied
    /// to a single object.
    pub const ADBC_OPTION_ISOLATION_LEVEL_LINEARIZABLE: &str =
        "adbc.connection.transaction.isolation.linearizable";
}

/// Utilities for driver info
///
/// For use with [crate::interface::ConnectionApi::get_info].
pub mod info {
    use arrow::datatypes::{DataType, Field, Schema, UnionMode};

    /// Contains known info codes defined by ADBC.
    pub mod codes {
        /// The database vendor/product version (type: utf8).
        pub const VENDOR_NAME: u32 = 0;
        /// The database vendor/product version (type: utf8).
        pub const VENDOR_VERSION: u32 = 1;
        /// The database vendor/product Arrow library version (type: utf8).
        pub const VENDOR_ARROW_VERSION: u32 = 2;
        /// The driver name (type: utf8).
        pub const DRIVER_NAME: u32 = 100;
        /// The driver version (type: utf8).
        pub const DRIVER_VERSION: u32 = 101;
        /// The driver Arrow library version (type: utf8).
        pub const DRIVER_ARROW_VERSION: u32 = 102;
    }
    pub fn info_schema() -> Schema {
        Schema::new(vec![
            Field::new("info_name", DataType::UInt32, false),
            Field::new(
                "info_value",
                DataType::Union(
                    vec![
                        Field::new("string_value", DataType::Utf8, true),
                        Field::new("bool_value", DataType::Boolean, true),
                        Field::new("int64_value", DataType::Int64, true),
                        Field::new("int32_bitmask", DataType::Int32, true),
                        Field::new(
                            "string_list",
                            DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                            true,
                        ),
                        Field::new(
                            "int32_to_int32_list_map",
                            DataType::Map(
                                Box::new(Field::new(
                                    "entries",
                                    DataType::Struct(vec![
                                        Field::new("key", DataType::Int32, false),
                                        Field::new(
                                            "value",
                                            DataType::List(Box::new(Field::new(
                                                "item",
                                                DataType::Int32,
                                                true,
                                            ))),
                                            true,
                                        ),
                                    ]),
                                    true,
                                )),
                                false,
                            ),
                            true,
                        ),
                    ],
                    vec![0, 1, 2, 3, 4, 5],
                    UnionMode::Dense,
                ),
                true,
            ),
        ])
    }
}
