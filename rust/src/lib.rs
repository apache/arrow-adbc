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

//! Arrow Database Connectivity (ADBC) allows efficient connections to databases
//! for OLAP workloads:
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
//! There are two flavors of ADBC that this library supports:
//!
//!  * **Native Rust implementations**. These implement the traits at the top level of
//!    this crate, starting with [AdbcDatabase].
//!  * **C API ADBC drivers**. These can be implemented in any language (that compiles
//!    to native code) and can be used by any language.
//!
//! # Native Rust drivers
//!
//! Native Rust drivers will implement the traits:
//!
//!  * [AdbcDatabase]
//!  * [AdbcConnection]
//!  * [AdbcStatement]
//!
//! For drivers implemented in Rust, using these will be more efficient and safe,
//! since it avoids the overhead of going through C FFI.
//!
//! # Using C API drivers
//!
//! The [driver_manager] module allows loading drivers, either from an initialization
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
//! # Creating C API drivers
//!
//! To implement an ADBC driver with a C interface, use the [implement] module. The macro
//! [adbc_init_func] will generate adapters from the safe Rust traits you implement
//! to the FFI interface recognized by ADBC.
//!
pub mod driver_manager;
pub mod error;
pub mod ffi;
pub mod implement;
pub mod info;
pub mod objects;
pub(crate) mod utils;

pub const ADBC_VERSION_1_0_0: i32 = 1000000;

use std::collections::HashMap;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

use crate::error::AdbcError;
use crate::info::{InfoCode, InfoData};

/// Databases hold state shared by multiple connections. This typically means
/// configuration and caches. For in-memory databases, it provides a place to
/// hold ownership of the in-memory database.
pub trait AdbcDatabase {
    type ConnectionType: AdbcConnection;

    /// Set an option on the database.
    ///
    /// Some databases may not allow setting options after it has been initialized.
    fn set_option(&self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<(), AdbcError>;

    /// Initialize a connection to the database.
    ///
    /// `options` provided will configure the connection, including the isolation
    /// level. See standard options in [options].
    fn connect<K, V>(
        &self,
        options: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self::ConnectionType, AdbcError>
    where
        K: AsRef<str>,
        V: AsRef<str>;
}

/// A connection is a single connection to a database.
///
/// It is never accessed concurrently from multiple threads.
///
/// # Autocommit
///
/// Connections should start in autocommit mode. They can be moved out by
/// setting [options::AdbcOptionKey::AutoCommit] to `"false"` (using
/// [AdbcConnection::set_option]). Turning off autocommit allows customizing
/// the isolation level. Read more in [adbc.h](https://github.com/apache/arrow-adbc/blob/main/adbc.h).
pub trait AdbcConnection {
    type StatementType: AdbcStatement;
    type ObjectCollectionType: objects::DatabaseCatalogCollection;

    /// Set an option on the connection.
    ///
    /// Some connections may not allow setting options after it has been initialized.
    fn set_option(&self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<(), AdbcError>;

    /// Create a new [AdbcStatement].
    fn new_statement(&self) -> Result<Self::StatementType, AdbcError>;

    /// Get metadata about the database/driver.
    ///
    /// If None is passed for `info_codes`, the method will return all info.
    /// Otherwise will return the specified info, in any order. If an unrecognized
    /// code is passed, it will return an error.
    ///
    /// Each metadatum is identified by an integer code.  The recognized
    /// codes are defined as constants.  Codes [0, 10_000) are reserved
    /// for ADBC usage.  Drivers/vendors will ignore requests for
    /// unrecognized codes (the row will be omitted from the result).
    /// Known codes are provided in [info::InfoCode].
    fn get_info(
        &self,
        info_codes: Option<&[InfoCode]>,
    ) -> Result<HashMap<InfoCode, InfoData>, AdbcError>;

    /// Get a single data base metadata. See [AdbcConnection::get_info()].
    ///
    /// Will return `None` if the code is not recognized.
    fn get_single_info(&self, info_code: InfoCode) -> Result<Option<InfoData>, AdbcError> {
        let info_codes = &[info_code];
        Ok(self
            .get_info(Some(info_codes.as_slice()))?
            .into_iter()
            .next()
            .map(|(_, val)| val))
    }

    /// Get a hierarchical view of all catalogs, database schemas, tables, and columns.
    ///
    /// # Parameters
    ///
    /// * **depth**: The level of nesting to display. If [AdbcObjectDepth::All], display
    ///   all levels. If [AdbcObjectDepth::Catalogs], display only catalogs (i.e.  `catalog_schemas`
    ///   will be null). If [AdbcObjectDepth::DBSchemas], display only catalogs and schemas
    ///   (i.e. `db_schema_tables` will be null), and so on.
    /// * **catalog**: Only show tables in the given catalog. If None,
    ///   do not filter by catalog. If an empty string, only show tables
    ///   without a catalog.  May be a search pattern (see next section).
    /// * **db_schema**: Only show tables in the given database schema. If
    ///   None, do not filter by database schema. If an empty string, only show
    ///   tables without a database schema. May be a search pattern (see next section).
    /// * **table_name**: Only show tables with the given name. If None, do not
    ///   filter by name. May be a search pattern (see next section).
    /// * **table_type**: Only show tables matching one of the given table
    ///   types. If None, show tables of any type. Valid table types should
    ///   match those returned by [AdbcConnection::get_table_schema].
    /// * **column_name**: Only show columns with the given name. If
    ///   None, do not filter by name.  May be a search pattern (see next section).
    ///
    /// # Search patterns
    ///
    /// Some parameters accept "search patterns", which are
    /// strings that can contain the special character `"%"` to match zero
    /// or more characters, or `"_"` to match exactly one character.  (See
    /// the documentation of DatabaseMetaData in JDBC or "Pattern Value
    /// Arguments" in the ODBC documentation.)
    fn get_objects(
        &self,
        depth: AdbcObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<Self::ObjectCollectionType, AdbcError>;

    /// Get the Arrow schema of a table.
    ///
    /// `catalog` or `db_schema` may be `None` when not applicable.
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema, AdbcError>;

    /// Get a list of table types in the database.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name       | Field Type
    /// -----------------|--------------
    /// `table_type`     | `utf8 not null`
    fn get_table_types(&self) -> Result<Vec<String>, AdbcError>;

    /// Read part of a partitioned result set.
    fn read_partition(
        &self,
        partition: &[u8],
    ) -> Result<Box<dyn RecordBatchReader + Send>, AdbcError>;

    /// Commit any pending transactions. Only used if autocommit is disabled.
    fn commit(&self) -> Result<(), AdbcError>;

    /// Roll back any pending transactions. Only used if autocommit is disabled.
    fn rollback(&self) -> Result<(), AdbcError>;
}

/// Depth parameter for GetObjects method.
#[derive(Debug, Copy, Clone)]
#[repr(i32)]
pub enum AdbcObjectDepth {
    /// Metadata on catalogs, schemas, tables, and columns.
    All = 0,
    /// Metadata on catalogs only.
    Catalogs = 1,
    /// Metadata on catalogs and schemas.
    DBSchemas = 2,
    /// Metadata on catalogs, schemas, and tables.
    Tables = 3,
}

/// A container for all state needed to execute a database query, such as the
/// query itself, parameters for prepared statements, driver parameters, etc.
///
/// Statements may represent queries or prepared statements.
///
/// Statements may be used multiple times and can be reconfigured
/// (e.g. they can be reused to execute multiple different queries).
/// However, executing a statement (and changing certain other state)
/// will invalidate result sets obtained prior to that execution.
///
/// Multiple statements may be created from a single connection.
/// However, the driver may block or error if they are used
/// concurrently (whether from a single thread or multiple threads).
pub trait AdbcStatement {
    /// Turn this statement into a prepared statement to be executed multiple time.
    ///
    /// This should return an error if called before [AdbcStatement::set_sql_query].
    fn prepare(&mut self) -> Result<(), AdbcError>;

    /// Set a string option on a statement.
    fn set_option(&mut self, key: impl AsRef<str>, value: impl AsRef<str>)
        -> Result<(), AdbcError>;

    /// Set the SQL query to execute.
    fn set_sql_query(&mut self, query: &str) -> Result<(), AdbcError>;

    /// Set the Substrait plan to execute.
    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<(), AdbcError>;

    /// Get the schema for bound parameters.
    ///
    /// This retrieves an Arrow schema describing the number, names, and
    /// types of the parameters in a parameterized statement.  The fields
    /// of the schema should be in order of the ordinal position of the
    /// parameters; named parameters should appear only once.
    ///
    /// If the parameter does not have a name, or the name cannot be
    /// determined, the name of the corresponding field in the schema will
    /// be an empty string.  If the type cannot be determined, the type of
    /// the corresponding field will be NA (NullType).
    ///
    /// This should return an error if this was called before [AdbcStatement::prepare].
    fn get_param_schema(&mut self) -> Result<Schema, AdbcError>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_data(&mut self, batch: RecordBatch) -> Result<(), AdbcError>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_stream(&mut self, stream: Box<dyn RecordBatchReader + Send>) -> Result<(), AdbcError>;

    /// Execute a statement and get the results.
    ///
    /// See [StatementResult].
    fn execute(&mut self) -> Result<StatementResult, AdbcError>;

    /// Execute a query that doesn't have a result set.
    ///
    /// Will return the number of rows affected. If the affected row count is
    /// unknown or unsupported by the database, will return `Ok(-1)`.
    fn execute_update(&mut self) -> Result<i64, AdbcError>;

    /// Execute a statement with a partitioned result set.
    ///
    /// This is not required to be implemented, as it only applies to backends
    /// that internally partition results. These backends can use this method
    /// to support threaded or distributed clients.
    ///
    /// See [PartitionedStatementResult].
    fn execute_partitioned(&mut self) -> Result<PartitionedStatementResult, AdbcError>;
}

#[cfg(substrait)]
pub trait AdbcStatementSubstrait: AdbcStatement {
    /// Set the Substrait plan to execute.
    fn set_substrait_plan(&mut self, plan: substrait::proto::Plan) -> Result<(), AdbcError>;
}

/// Result of calling [AdbcStatement::execute].
///
/// `result` may be None if there is no meaningful result.
/// `row_affected` may be -1 if not applicable or if it is not supported.
pub struct StatementResult {
    pub result: Option<Box<dyn RecordBatchReader + Send>>,
    pub rows_affected: i64,
}

/// Partitioned results
///
/// [AdbcConnection::read_partition] will be called to get the output stream
/// for each partition.
///
/// These may be used by a multi-threaded or a distributed client. Each partition
/// will be retrieved by a separate connection. For in-memory databases, these
/// may be connections on different threads that all reference the same database.
/// For remote databases, these may be connections in different processes.
#[derive(Debug, Clone)]
pub struct PartitionedStatementResult {
    pub schema: Schema,
    pub partition_ids: Vec<Vec<u8>>,
    pub rows_affected: i64,
}

/// Known options that can be set on databases, connections, and statements.
///
/// For use with [crate::AdbcDatabase::set_option],
/// [crate::AdbcConnection::set_option],
/// and [crate::AdbcStatement::set_option].
pub mod options {
    /// Various known options for ADBC connections.
    ///
    /// These convert to canonical option strings as defined in the C API.
    pub enum AdbcOptionKey {
        /// When ingesting a data stream, table name to write to.
        IngestTargetTable,
        /// How to ingest a table. See [IngestMode] for canonical possible values.
        IngestMode,
        /// Whether autocommit is enabled.
        AutoCommit,
        /// Whether the current connection should be restricted to being read-only.
        ReadOnly,
        /// The name of the canonical option for setting the isolation level of a
        /// transaction.
        ///
        /// Should only be used in conjunction with autocommit disabled and
        /// AdbcConnectionCommit / AdbcConnectionRollback. If the desired
        /// isolation level is not supported by a driver, it should return an
        /// appropriate error.
        ///
        /// See [IsolationLevel] for possible values.
        IsolationLevel,
    }

    impl AsRef<str> for AdbcOptionKey {
        fn as_ref(&self) -> &str {
            match self {
                Self::IngestTargetTable => "adbc.ingest.target_table",
                Self::IngestMode => "adbc.ingest.mode",
                Self::AutoCommit => "adbc.connection.autocommit",
                Self::ReadOnly => "adbc.connection.readonly",
                Self::IsolationLevel => "adbc.connection.transaction.isolation_level",
            }
        }
    }

    /// Possible ingest mode for use with option [AdbcOptionKey::IngestMode].
    ///
    /// These convert to canonical option strings as defined in the C API.
    pub enum IngestMode {
        Create,
        Append,
    }

    impl AsRef<str> for IngestMode {
        fn as_ref(&self) -> &str {
            match self {
                Self::Create => "adbc.ingest.mode.create",
                Self::Append => "adbc.ingest.mode.append",
            }
        }
    }

    /// Possible isolation level values for use with option [AdbcOptionKey::IsolationLevel].
    pub enum IsolationLevel {
        /// Use database or driver default isolation level
        Default,
        /// The lowest isolation level. Dirty reads are allowed, so one transaction
        /// may see not-yet-committed changes made by others.
        ReadUncommitted,
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
        ReadCommitted,
        /// Lock-based concurrency control keeps read AND write locks
        /// (acquired on selection data) until the end of the transaction.
        ///
        /// However, range-locks are not managed, so phantom reads can occur.
        /// Write skew is possible at this isolation level in some systems.
        RepeatableRead,
        /// This isolation guarantees that all reads in the transaction
        /// will see a consistent snapshot of the database and the transaction
        /// should only successfully commit if no updates conflict with any
        /// concurrent updates made since that snapshot.
        Snapshot,
        /// Serializability requires read and write locks to be released
        /// only at the end of the transaction. This includes acquiring range-
        /// locks when a select query uses a ranged WHERE clause to avoid
        /// phantom reads.
        Serializable,
        /// The central distinction between serializability and linearizability
        /// is that serializability is a global property; a property of an entire
        /// history of operations and transactions. Linearizability is a local
        /// property; a property of a single operation/transaction.
        ///
        /// Linearizability can be viewed as a special case of strict serializability
        /// where transactions are restricted to consist of a single operation applied
        /// to a single object.
        Linearizable,
    }

    impl AsRef<str> for IsolationLevel {
        fn as_ref(&self) -> &str {
            match self {
                Self::Default => "adbc.connection.transaction.isolation.default",
                Self::ReadUncommitted => "adbc.connection.transaction.isolation.read_uncommitted",
                Self::ReadCommitted => "adbc.connection.transaction.isolation.read_committed",
                Self::RepeatableRead => "adbc.connection.transaction.isolation.repeatable_read",
                Self::Snapshot => "adbc.connection.transaction.isolation.snapshot",
                Self::Serializable => "adbc.connection.transaction.isolation.serializable",
                Self::Linearizable => "adbc.connection.transaction.isolation.linearizable",
            }
        }
    }
}
