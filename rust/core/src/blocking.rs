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

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::Schema;

use crate::error::Result;
use crate::executor::AsyncExecutor;
use crate::non_blocking::PartitionedResult;
use crate::options::{self, OptionConnection, OptionDatabase, OptionStatement, OptionValue};
use crate::{
    LocalAsyncConnection, LocalAsyncDatabase, LocalAsyncDriver, LocalAsyncOptionable,
    LocalAsyncStatement,
};

/// Ability to configure an object by setting/getting options.
pub trait Optionable {
    type Option: AsRef<str>;

    /// Set a post-init option.
    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()>;

    /// Get a string option value by key.
    fn get_option_string(&self, key: Self::Option) -> Result<String>;

    /// Get a bytes option value by key.
    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>>;

    /// Get an integer option value by key.
    fn get_option_int(&self, key: Self::Option) -> Result<i64>;

    /// Get a float option value by key.
    fn get_option_double(&self, key: Self::Option) -> Result<f64>;
}

/// A handle to an ADBC driver.
pub trait Driver {
    type DatabaseType: Database;

    /// Allocate and initialize a new database without pre-init options.
    fn new_database(&mut self) -> Result<Self::DatabaseType>;

    /// Allocate and initialize a new database with pre-init options.
    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType>;
}

/// A handle to an ADBC database.
///
/// Databases hold state shared by multiple connections. This typically means
/// configuration and caches. For in-memory databases, it provides a place to
/// hold ownership of the in-memory database.
///
/// Databases must be kept alive as long as any connections exist.
pub trait Database: Optionable<Option = OptionDatabase> {
    type ConnectionType: Connection;

    /// Allocate and initialize a new connection without pre-init options.
    fn new_connection(&self) -> Result<Self::ConnectionType>;

    /// Allocate and initialize a new connection with pre-init options.
    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (options::OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType>;
}

/// A handle to an ADBC connection.
///
/// Connections provide methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// # Autocommit
///
/// Connections should start in autocommit mode. They can be moved out by
/// setting [options::OptionConnection::AutoCommit] to "false". Turning off
/// autocommit allows customizing the isolation level.
pub trait Connection: Optionable<Option = OptionConnection> {
    type StatementType: Statement;

    /// Allocate and initialize a new statement.
    fn new_statement(&mut self) -> Result<Self::StatementType>;

    /// Cancel the in-progress operation on a connection.
    fn cancel(&mut self) -> Result<()>;

    /// Get metadata about the database/driver.
    ///
    /// # Arguments
    ///
    /// - `codes` - Requested metadata. If `None`, retrieve all available metadata.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name                  | Field Type
    /// ----------------------------|------------------------
    /// info_name                   | uint32 not null
    /// info_value                  | INFO_SCHEMA
    ///
    /// INFO_SCHEMA is a dense union with members:
    ///
    /// Field Name (Type Code)      | Field Type
    /// ----------------------------|------------------------
    /// string_value (0)            | utf8
    /// bool_value (1)              | bool
    /// int64_value (2)             | int64
    /// int32_bitmask (3)           | int32
    /// string_list (4)             | list\<utf8\>
    /// int32_to_int32_list_map (5) | map\<int32, list\<int32\>\>
    fn get_info(
        &self,
        codes: Option<HashSet<options::InfoCode>>,
    ) -> Result<impl RecordBatchReader + Send>;

    /// Get a hierarchical view of all catalogs, database schemas, tables, and
    /// columns.
    ///
    /// # Arguments
    ///
    /// - `depth` - The level of nesting to query.
    /// - `catalog` - Only show tables in the given catalog. If `None`,
    ///   do not filter by catalog. If an empty string, only show tables
    ///   without a catalog.  May be a search pattern.
    /// - `db_schema` - Only show tables in the given database schema. If
    ///   `None`, do not filter by database schema. If an empty string, only show
    ///   tables without a database schema. May be a search pattern.
    /// - `table_name` - Only show tables with the given name. If `None`, do not
    ///   filter by name. May be a search pattern.
    /// - `table_type` - Only show tables matching one of the given table
    ///   types. If `None`, show tables of any type. Valid table types can be fetched
    ///   from [Connection::get_table_types].
    /// - `column_name` - Only show columns with the given name. If
    ///   `None`, do not filter by name.  May be a search pattern..
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// | Field Name               | Field Type               |
    /// |--------------------------|--------------------------|
    /// | catalog_name             | utf8                     |
    /// | catalog_db_schemas       | list\<DB_SCHEMA_SCHEMA\> |
    ///
    /// DB_SCHEMA_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | db_schema_name           | utf8                    |
    /// | db_schema_tables         | list\<TABLE_SCHEMA\>    |
    ///
    /// TABLE_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type                |
    /// |--------------------------|---------------------------|
    /// | table_name               | utf8 not null             |
    /// | table_type               | utf8 not null             |
    /// | table_columns            | list\<COLUMN_SCHEMA\>     |
    /// | table_constraints        | list\<CONSTRAINT_SCHEMA\> |
    ///
    /// COLUMN_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              | Comments |
    /// |--------------------------|-------------------------|----------|
    /// | column_name              | utf8 not null           |          |
    /// | ordinal_position         | int32                   | (1)      |
    /// | remarks                  | utf8                    | (2)      |
    /// | xdbc_data_type           | int16                   | (3)      |
    /// | xdbc_type_name           | utf8                    | (3)      |
    /// | xdbc_column_size         | int32                   | (3)      |
    /// | xdbc_decimal_digits      | int16                   | (3)      |
    /// | xdbc_num_prec_radix      | int16                   | (3)      |
    /// | xdbc_nullable            | int16                   | (3)      |
    /// | xdbc_column_def          | utf8                    | (3)      |
    /// | xdbc_sql_data_type       | int16                   | (3)      |
    /// | xdbc_datetime_sub        | int16                   | (3)      |
    /// | xdbc_char_octet_length   | int32                   | (3)      |
    /// | xdbc_is_nullable         | utf8                    | (3)      |
    /// | xdbc_scope_catalog       | utf8                    | (3)      |
    /// | xdbc_scope_schema        | utf8                    | (3)      |
    /// | xdbc_scope_table         | utf8                    | (3)      |
    /// | xdbc_is_autoincrement    | bool                    | (3)      |
    /// | xdbc_is_generatedcolumn  | bool                    | (3)      |
    ///
    /// 1. The column's ordinal position in the table (starting from 1).
    /// 2. Database-specific description of the column.
    /// 3. Optional value.  Should be null if not supported by the driver.
    ///    `xdbc_` values are meant to provide JDBC/ODBC-compatible metadata
    ///    in an agnostic manner.
    ///
    /// CONSTRAINT_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              | Comments |
    /// |--------------------------|-------------------------|----------|
    /// | constraint_name          | utf8                    |          |
    /// | constraint_type          | utf8 not null           | (1)      |
    /// | constraint_column_names  | list\<utf8\> not null     | (2)      |
    /// | constraint_column_usage  | list\<USAGE_SCHEMA\>      | (3)      |
    ///
    /// 1. One of `CHECK`, `FOREIGN KEY`, `PRIMARY KEY`, or `UNIQUE`.
    /// 2. The columns on the current table that are constrained, in
    ///    order.
    /// 3. For `FOREIGN KEY` only, the referenced table and columns.
    ///
    /// USAGE_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type              |
    /// |--------------------------|-------------------------|
    /// | fk_catalog               | utf8                    |
    /// | fk_db_schema             | utf8                    |
    /// | fk_table                 | utf8 not null           |
    /// | fk_column_name           | utf8 not null           |
    ///
    fn get_objects(
        &self,
        depth: options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send>;

    /// Get the Arrow schema of a table.
    ///
    /// # Arguments
    ///
    /// - `catalog` - The catalog (or `None` if not applicable).
    /// - `db_schema` - The database schema (or `None` if not applicable).
    /// - `table_name` - The table name.
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema>;

    /// Get a list of table types in the database.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name     | Field Type
    /// ---------------|--------------
    /// table_type     | utf8 not null
    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send>;

    /// Get the names of statistics specific to this driver.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name     | Field Type
    /// ---------------|----------------
    /// statistic_name | utf8 not null
    /// statistic_key  | int16 not null
    ///
    /// # Since
    /// ADBC API revision 1.1.0
    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send>;

    /// Get statistics about the data distribution of table(s).
    ///
    /// # Arguments
    ///
    /// - `catalog` - The catalog (or `None` if not applicable). May be a search pattern.
    /// - `db_schema` - The database schema (or `None` if not applicable). May be a search pattern
    /// - `table_name` - The table name (or `None` if not applicable). May be a search pattern
    /// - `approximate` - If false, request exact values of statistics, else
    ///   allow for best-effort, approximate, or cached values. The database may
    ///   return approximate values regardless, as indicated in the result.
    ///   Requesting exact values may be expensive or unsupported.
    ///
    /// # Result
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// | Field Name               | Field Type                       |
    /// |--------------------------|----------------------------------|
    /// | catalog_name             | utf8                             |
    /// | catalog_db_schemas       | list\<DB_SCHEMA_SCHEMA\> not null|
    ///
    /// DB_SCHEMA_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type                        |
    /// |--------------------------|-----------------------------------|
    /// | db_schema_name           | utf8                              |
    /// | db_schema_statistics     | list\<STATISTICS_SCHEMA\> not null|
    ///
    /// STATISTICS_SCHEMA is a Struct with fields:
    ///
    /// | Field Name               | Field Type                       | Comments |
    /// |--------------------------|----------------------------------| -------- |
    /// | table_name               | utf8 not null                    |          |
    /// | column_name              | utf8                             | (1)      |
    /// | statistic_key            | int16 not null                   | (2)      |
    /// | statistic_value          | VALUE_SCHEMA not null            |          |
    /// | statistic_is_approximate | bool not null                    | (3)      |
    ///
    /// 1. If null, then the statistic applies to the entire table.
    /// 2. A dictionary-encoded statistic name (although we do not use the Arrow
    ///    dictionary type). Values in [0, 1024) are reserved for ADBC.  Other
    ///    values are for implementation-specific statistics.  For the definitions
    ///    of predefined statistic types, see [options::Statistics]. To get
    ///    driver-specific statistic names, use [Connection::get_statistic_names].
    /// 3. If true, then the value is approximate or best-effort.
    ///
    /// VALUE_SCHEMA is a dense union with members:
    ///
    /// | Field Name               | Field Type                       |
    /// |--------------------------|----------------------------------|
    /// | int64                    | int64                            |
    /// | uint64                   | uint64                           |
    /// | float64                  | float64                          |
    /// | binary                   | binary                           |
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader + Send>;

    /// Commit any pending transactions. Only used if autocommit is disabled.
    ///
    /// Behavior is undefined if this is mixed with SQL transaction statements.
    fn commit(&mut self) -> Result<()>;

    /// Roll back any pending transactions. Only used if autocommit is disabled.
    ///
    /// Behavior is undefined if this is mixed with SQL transaction statements.
    fn rollback(&mut self) -> Result<()>;

    /// Retrieve a given partition of data.
    ///
    /// A partition can be retrieved from [Statement::execute_partitions].
    ///
    /// # Arguments
    ///
    /// - `partition` - The partition descriptor.
    fn read_partition(&self, partition: &[u8]) -> Result<impl RecordBatchReader + Send>;
}

/// A handle to an ADBC statement.
///
/// A statement is a container for all state needed to execute a database query,
/// such as the query itself, parameters for prepared statements, driver
/// parameters, etc.
///
/// Statements may represent queries or prepared statements.
///
/// Statements may be used multiple times and can be reconfigured
/// (e.g. they can be reused to execute multiple different queries).
/// However, executing a statement (and changing certain other state)
/// will invalidate result sets obtained prior to that execution.
///
/// Multiple statements may be created from a single connection.
/// However, the driver may block or error if they are used concurrently
/// (whether from a single thread or multiple threads).
pub trait Statement: Optionable<Option = OptionStatement> {
    /// Bind Arrow data. This can be used for bulk inserts or prepared
    /// statements.
    fn bind(&mut self, batch: RecordBatch) -> Result<()>;

    /// Bind Arrow data. This can be used for bulk inserts or prepared
    /// statements.
    // TODO(alexandreyc): should we use a generic here instead of a trait object?
    // See: https://github.com/apache/arrow-adbc/pull/1725#discussion_r1567750972
    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()>;

    /// Execute a statement and get the results.
    ///
    /// This invalidates any prior result sets.
    // TODO(alexandreyc): is the Send bound absolutely necessary? same question
    // for all methods that return an impl RecordBatchReader
    // See: https://github.com/apache/arrow-adbc/pull/1725#discussion_r1567748242
    fn execute(&mut self) -> Result<impl RecordBatchReader + Send>;

    /// Execute a statement that doesn’t have a result set and get the number
    /// of affected rows.
    ///
    /// This invalidates any prior result sets.
    ///
    /// # Result
    ///
    /// Will return the number of rows affected. If the affected row count is
    /// unknown or unsupported by the database, will return `None`.
    fn execute_update(&mut self) -> Result<Option<i64>>;

    /// Get the schema of the result set of a query without executing it.
    ///
    /// This invalidates any prior result sets.
    ///
    /// Depending on the driver, this may require first executing
    /// [Statement::prepare].
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    fn execute_schema(&mut self) -> Result<Schema>;

    /// Execute a statement and get the results as a partitioned result set.
    fn execute_partitions(&mut self) -> Result<PartitionedResult>;

    /// Get the schema for bound parameters.
    ///
    /// This retrieves an Arrow schema describing the number, names, and
    /// types of the parameters in a parameterized statement. The fields
    /// of the schema should be in order of the ordinal position of the
    /// parameters; named parameters should appear only once.
    ///
    /// If the parameter does not have a name, or the name cannot be
    /// determined, the name of the corresponding field in the schema will
    /// be an empty string. If the type cannot be determined, the type of
    /// the corresponding field will be NA (NullType).
    ///
    /// This should be called after [Statement::prepare].
    fn get_parameter_schema(&self) -> Result<Schema>;

    /// Turn this statement into a prepared statement to be executed multiple
    /// times.
    ///
    /// This invalidates any prior result sets.
    fn prepare(&mut self) -> Result<()>;

    /// Set the SQL query to execute.
    ///
    /// The query can then be executed with [Statement::execute]. For queries
    /// expected to be executed repeatedly, call [Statement::prepare] first.
    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()>;

    /// Set the Substrait plan to execute.
    ///
    /// The query can then be executed with [Statement::execute]. For queries
    /// expected to be executed repeatedly, call [Statement::prepare] first.
    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()>;

    /// Cancel execution of an in-progress query.
    ///
    /// This can be called during [Statement::execute] (or similar), or while
    /// consuming a result set returned from such.
    ///
    /// # Since
    ///
    /// ADBC API revision 1.1.0
    fn cancel(&mut self) -> Result<()>;
}

pub struct SyncDriverWrapper<A, D> {
    inner: D,
    executor: Arc<A>,
}

impl<A: AsyncExecutor, D: Default> Default for SyncDriverWrapper<A, D> {
    fn default() -> Self {
        Self {
            inner: Default::default(),
            executor: Arc::new(A::new(Default::default()).expect("failed to create executor")),
        }
    }
}

impl<A, D> Driver for SyncDriverWrapper<A, D>
where
    A: AsyncExecutor,
    D: LocalAsyncDriver,
    D::DatabaseType: LocalAsyncDatabase + LocalAsyncOptionable<Option = OptionDatabase>,
    <D::DatabaseType as LocalAsyncDatabase>::ConnectionType:
        LocalAsyncConnection + LocalAsyncOptionable<Option = OptionConnection>,
    <<D::DatabaseType as LocalAsyncDatabase>::ConnectionType as LocalAsyncConnection>::StatementType: LocalAsyncOptionable<Option = OptionStatement>,
{
    type DatabaseType = SyncDatabaseWrapper<A, D::DatabaseType>;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        let db = self.executor.block_on(self.inner.new_database())?;

        Ok(SyncDatabaseWrapper { inner: db, executor: self.executor.clone() })
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let db = self.executor.block_on(self.inner.new_database_with_opts(opts.into_iter().collect()))?;

        Ok(SyncDatabaseWrapper { inner: db, executor: self.executor.clone() })
    }
}

pub struct SyncDatabaseWrapper<A, DB> {
    inner: DB,
    executor: Arc<A>,
}

impl<A, DB> Optionable for SyncDatabaseWrapper<A, DB>
where
    A: AsyncExecutor,
    DB: LocalAsyncOptionable<Option = OptionDatabase>,
{
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.executor.block_on(self.inner.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.executor.block_on(self.inner.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.executor.block_on(self.inner.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.executor.block_on(self.inner.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.executor.block_on(self.inner.get_option_double(key))
    }
}

impl<A, DB> Database for SyncDatabaseWrapper<A, DB>
where
    A: AsyncExecutor,
    DB: LocalAsyncDatabase + LocalAsyncOptionable<Option = OptionDatabase>,
    DB::ConnectionType: LocalAsyncConnection + LocalAsyncOptionable<Option = OptionConnection>,
    <DB::ConnectionType as LocalAsyncConnection>::StatementType:
        LocalAsyncOptionable<Option = OptionStatement>,
{
    type ConnectionType = SyncConnectionWrapper<A, DB::ConnectionType>;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let conn = self.executor.block_on(self.inner.new_connection())?;

        Ok(SyncConnectionWrapper {
            inner: conn,
            executor: self.executor.clone(),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (options::OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let conn = self.executor.block_on(
            self.inner
                .new_connection_with_opts(opts.into_iter().collect()),
        )?;

        Ok(SyncConnectionWrapper {
            inner: conn,
            executor: self.executor.clone(),
        })
    }
}

pub struct SyncConnectionWrapper<A, Conn> {
    inner: Conn,
    executor: Arc<A>,
}

impl<A, Conn> Optionable for SyncConnectionWrapper<A, Conn>
where
    A: AsyncExecutor,
    Conn: LocalAsyncOptionable<Option = OptionConnection>,
{
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.executor.block_on(self.inner.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.executor.block_on(self.inner.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.executor.block_on(self.inner.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.executor.block_on(self.inner.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.executor.block_on(self.inner.get_option_double(key))
    }
}

impl<A, Conn> Connection for SyncConnectionWrapper<A, Conn>
where
    A: AsyncExecutor,
    Conn: LocalAsyncConnection + LocalAsyncOptionable<Option = OptionConnection>,
    Conn::StatementType: LocalAsyncOptionable<Option = OptionStatement>,
{
    type StatementType = SyncStatementWrapper<A, Conn::StatementType>;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let stmt = self.executor.block_on(self.inner.new_statement())?;

        Ok(SyncStatementWrapper {
            inner: stmt,
            executor: self.executor.clone(),
        })
    }

    fn cancel(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.cancel())
    }

    fn get_info(
        &self,
        codes: Option<HashSet<options::InfoCode>>,
    ) -> Result<impl RecordBatchReader + Send> {
        self.executor.block_on(self.inner.get_info(codes))
    }

    fn get_objects(
        &self,
        depth: options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        self.executor.block_on(self.inner.get_objects(
            depth,
            catalog,
            db_schema,
            table_name,
            table_type,
            column_name,
        ))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema> {
        self.executor
            .block_on(self.inner.get_table_schema(catalog, db_schema, table_name))
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader + Send> {
        self.executor.block_on(self.inner.get_table_types())
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader + Send> {
        self.executor.block_on(self.inner.get_statistic_names())
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader + Send> {
        self.executor.block_on(self.inner.get_statistics(
            catalog,
            db_schema,
            table_name,
            approximate,
        ))
    }

    fn commit(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.commit())
    }

    fn rollback(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.rollback())
    }

    fn read_partition(&self, partition: &[u8]) -> Result<impl RecordBatchReader + Send> {
        self.executor.block_on(self.inner.read_partition(partition))
    }
}

pub struct SyncStatementWrapper<A, T> {
    inner: T,
    executor: Arc<A>,
}

impl<A, Stmt> Optionable for SyncStatementWrapper<A, Stmt>
where
    A: AsyncExecutor,
    Stmt: LocalAsyncOptionable<Option = OptionStatement>,
{
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.executor.block_on(self.inner.set_option(key, value))
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.executor.block_on(self.inner.get_option_string(key))
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.executor.block_on(self.inner.get_option_bytes(key))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.executor.block_on(self.inner.get_option_int(key))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.executor.block_on(self.inner.get_option_double(key))
    }
}

impl<A, Stmt> Statement for SyncStatementWrapper<A, Stmt>
where
    A: AsyncExecutor,
    Stmt: LocalAsyncStatement + LocalAsyncOptionable<Option = OptionStatement>,
{
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        self.executor.block_on(self.inner.bind(batch))
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.executor.block_on(self.inner.bind_stream(reader))
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        self.executor.block_on(self.inner.execute())
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        self.executor.block_on(self.inner.execute_update())
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        self.executor.block_on(self.inner.execute_schema())
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        self.executor.block_on(self.inner.execute_partitions())
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        self.executor.block_on(self.inner.get_parameter_schema())
    }

    fn prepare(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.prepare())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        self.executor
            .block_on(self.inner.set_sql_query(query.as_ref()))
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()> {
        self.executor
            .block_on(self.inner.set_substrait_plan(plan.as_ref()))
    }

    fn cancel(&mut self) -> Result<()> {
        self.executor.block_on(self.inner.cancel())
    }
}
