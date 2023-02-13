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
    use arrow::{
        array::{
            as_primitive_array, as_string_array, as_union_array, Array, ArrayBuilder, ArrayRef,
            BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, MapBuilder, StringBuilder,
            UInt32BufferBuilder, UInt32Builder, UInt8BufferBuilder, UnionArray,
        },
        datatypes::{DataType, Field, Schema, UInt32Type, UnionMode},
        error::ArrowError,
        record_batch::{RecordBatch, RecordBatchReader},
    };
    use std::{borrow::Cow, collections::HashMap, sync::Arc};

    use crate::util::SingleBatchReader;

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
                                        Field::new("keys", DataType::Int32, false),
                                        Field::new(
                                            "values",
                                            DataType::List(Box::new(Field::new(
                                                "item",
                                                DataType::Int32,
                                                true,
                                            ))),
                                            true,
                                        ),
                                    ]),
                                    false,
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

    /// Rust representations of database/drier metadata
    #[derive(Clone, Debug, PartialEq)]
    pub enum InfoData {
        StringValue(Cow<'static, str>),
        BoolValue(bool),
        Int64Value(i64),
        Int32Bitmask(i32),
        StringList(Vec<String>),
        Int32ToInt32ListMap(HashMap<i32, Vec<i32>>),
    }

    pub fn export_info_data(
        info_iter: impl IntoIterator<Item = (u32, InfoData)>,
    ) -> Box<dyn RecordBatchReader> {
        let info_iter = info_iter.into_iter();

        let mut codes = UInt32Builder::with_capacity(info_iter.size_hint().0);

        // Type id tells which array the value is in
        let mut type_id = UInt8BufferBuilder::new(info_iter.size_hint().0);
        // Value offset tells the offset of the value in the respective array
        let mut value_offsets = UInt32BufferBuilder::new(info_iter.size_hint().0);

        // Make one builder per child of union array. Will combine after.
        let mut string_values = StringBuilder::new();
        let mut bool_values = BooleanBuilder::new();
        let mut int64_values = Int64Builder::new();
        let mut int32_bitmasks = Int32Builder::new();
        let mut string_lists = ListBuilder::new(StringBuilder::new());
        let mut int32_to_int32_list_maps = MapBuilder::new(
            None,
            Int32Builder::new(),
            ListBuilder::new(Int32Builder::new()),
        );

        for (code, info) in info_iter {
            codes.append_value(code);

            match info {
                InfoData::StringValue(val) => {
                    string_values.append_value(val);
                    type_id.append(0);
                    let value_offset = string_values.len() - 1;
                    value_offsets.append(
                        value_offset
                            .try_into()
                            .expect("Array has more values than can be indexed by u32"),
                    );
                }
                _ => {
                    todo!("support other types in info_data")
                }
            };
        }

        let arrays: Vec<ArrayRef> = vec![
            Arc::new(string_values.finish()),
            Arc::new(bool_values.finish()),
            Arc::new(int64_values.finish()),
            Arc::new(int32_bitmasks.finish()),
            Arc::new(string_lists.finish()),
            Arc::new(int32_to_int32_list_maps.finish()),
        ];
        let info_schema = info_schema();
        let union_fields = {
            match info_schema.field(1).data_type() {
                DataType::Union(fields, _, _) => fields,
                _ => unreachable!(),
            }
        };
        let children = union_fields
            .iter()
            .map(|f| f.to_owned())
            .zip(arrays.into_iter())
            .collect();
        let info_value = UnionArray::try_new(
            &[0, 1, 2, 3, 4, 5],
            type_id.finish(),
            Some(value_offsets.finish()),
            children,
        )
        .expect("Info value array is always valid.");

        // Make a record batch
        let batch: RecordBatch = RecordBatch::try_new(
            Arc::new(info_schema),
            vec![Arc::new(codes.finish()), Arc::new(info_value)],
        )
        .expect("Info data batch is always valid.");

        // Wrap record batch into a reader with std::iter::once
        Box::new(SingleBatchReader::new(batch))
    }

    pub fn import_info_data(
        reader: Box<dyn RecordBatchReader>,
    ) -> Result<Vec<(u32, InfoData)>, ArrowError> {
        let batches = reader.collect::<Result<Vec<RecordBatch>, ArrowError>>()?;

        Ok(batches
            .iter()
            .flat_map(|batch| {
                let codes = as_primitive_array::<UInt32Type>(batch.column(0));
                let codes = codes.into_iter().map(|code| code.unwrap());

                let info_data = as_union_array(batch.column(1));
                let info_data = (0..info_data.len()).map(|i| -> InfoData {
                    let type_id = info_data.type_id(i);
                    match type_id {
                        0 => InfoData::StringValue(Cow::Owned(
                            as_string_array(&info_data.value(i)).value(0).to_string(),
                        )),
                        _ => todo!("Support other types"),
                    }
                });

                std::iter::zip(codes, info_data)
            })
            .collect())
    }

    #[cfg(test)]
    mod test {
        use std::ops::Deref;

        use arrow::{
            array::{as_primitive_array, as_string_array, as_union_array},
            datatypes::UInt32Type,
        };

        use super::*;

        #[test]
        fn test_export_info_data() {
            let example_info = vec![
                (
                    codes::VENDOR_NAME,
                    InfoData::StringValue(Cow::Borrowed("test vendor")),
                ),
                (
                    codes::DRIVER_NAME,
                    InfoData::StringValue(Cow::Borrowed("test driver")),
                ),
            ];

            let info = export_info_data(example_info.clone());

            assert_eq!(info.schema().deref(), &info_schema());
            let info: HashMap<u32, String> = info
                .flat_map(|maybe_batch| {
                    let batch = maybe_batch.unwrap();
                    let id = as_primitive_array::<UInt32Type>(batch.column(0));
                    let values = as_union_array(batch.column(1));
                    let string_values = as_string_array(values.child(0));
                    let mut out = vec![];
                    for i in 0..batch.num_rows() {
                        assert_eq!(values.type_id(i), 0);
                        out.push((id.value(i), string_values.value(i).to_string()));
                    }
                    out
                })
                .collect();

            assert_eq!(
                info.get(&codes::VENDOR_NAME),
                Some(&"test vendor".to_string())
            );
            assert_eq!(
                info.get(&codes::DRIVER_NAME),
                Some(&"test driver".to_string())
            );

            let info = export_info_data(example_info);

            let info: HashMap<u32, InfoData> =
                import_info_data(info).unwrap().into_iter().collect();
            dbg!(&info);

            assert_eq!(
                info.get(&codes::VENDOR_NAME),
                Some(&InfoData::StringValue(Cow::Owned(
                    "test vendor".to_string()
                )))
            );
            assert_eq!(
                info.get(&codes::DRIVER_NAME),
                Some(&InfoData::StringValue(Cow::Owned(
                    "test driver".to_string()
                )))
            );
        }
    }
}

pub(crate) mod util {
    use std::sync::Arc;

    use arrow::{
        datatypes::Schema,
        error::ArrowError,
        record_batch::{RecordBatch, RecordBatchReader},
    };

    /// [RecordBatchReader] for a single record batch.
    pub(crate) struct SingleBatchReader {
        batch: Option<RecordBatch>,
        schema: Arc<Schema>,
    }

    impl SingleBatchReader {
        pub fn new(batch: RecordBatch) -> Self {
            let schema = batch.schema();
            Self {
                batch: Some(batch),
                schema,
            }
        }
    }

    impl Iterator for SingleBatchReader {
        type Item = Result<RecordBatch, ArrowError>;

        fn next(&mut self) -> Option<Self::Item> {
            Ok(self.batch.take()).transpose()
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            let left = if self.batch.is_some() { 1 } else { 0 };
            (left, Some(left))
        }
    }

    impl RecordBatchReader for SingleBatchReader {
        fn schema(&self) -> arrow::datatypes::SchemaRef {
            self.schema.clone()
        }
    }
}
