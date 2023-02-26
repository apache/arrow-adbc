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

//! API traits for ADBC structs
//!
//! These are the interfaces to ADBC structs made more ergonomic for Rust
//! developers. They are implemented by the structs in [crate::driver_manager].
use arrow::{datatypes::Schema, record_batch::RecordBatch, record_batch::RecordBatchReader};

use crate::{ffi::AdbcObjectDepth, info::InfoData};

/// Databases hold state shared by multiple connections. This typically means
/// configuration and caches. For in-memory databases, it provides a place to
/// hold ownership of the in-memory database.
pub trait DatabaseApi {
    type Error;

    /// Set an option on the database.
    ///
    /// Some databases may not allow setting options after it has been initialized.
    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error>;
}

/// A connection is a single connection to a database.
///
/// It is never accessed concurrently from multiple threads.
///
/// # Autocommit
///
/// Connections should start in autocommit mode. They can be moved out by
/// setting `"adbc.connection.autocommit"` to `"false"` (using
/// [ConnectionApi::set_option]). Turning off autocommit allows customizing
/// the isolation level. Read more in [adbc.h](https://github.com/apache/arrow-adbc/blob/main/adbc.h).
pub trait ConnectionApi {
    type Error;
    type ObjectCollectionType: objects::DatabaseCatalogCollection;

    /// Set an option on the connection.
    ///
    /// Some connections may not allow setting options after it has been initialized.
    fn set_option(&self, key: &str, value: &str) -> Result<(), Self::Error>;

    /// Get metadata about the database/driver.
    ///
    /// If None is passed for `info_codes`, the method will return all info.
    /// Otherwise will return the specified info, in any order. If an unrecognized
    /// code is passed, it will return an error.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name                  | Field Type
    /// ----------------------------|------------------------
    /// `info_name`                 | `uint32 not null`
    /// `info_value`                | `INFO_SCHEMA`
    ///
    /// `INFO_SCHEMA` is a dense union with members:
    ///
    /// Field Name (Type Code)        | Field Type
    /// ------------------------------|------------------------
    /// `string_value` (0)            | `utf8`
    /// `bool_value` (1)              | `bool`
    /// `int64_value` (2)             | `int64`
    /// `int32_bitmask` (3)           | `int32`
    /// `string_list` (4)             | `list<utf8>`
    /// `int32_to_int32_list_map` (5) | `map<int32, list<int32>>`
    ///
    /// Each metadatum is identified by an integer code.  The recognized
    /// codes are defined as constants.  Codes [0, 10_000) are reserved
    /// for ADBC usage.  Drivers/vendors will ignore requests for
    /// unrecognized codes (the row will be omitted from the result).
    ///
    /// For definitions of known ADBC codes, see <https://github.com/apache/arrow-adbc/blob/main/adbc.h>
    fn get_info(&self, info_codes: Option<&[u32]>) -> Result<Vec<(u32, InfoData)>, Self::Error>;

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
    ///   match those returned by [ConnectionApi::get_table_schema].
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
    ) -> Result<Self::ObjectCollectionType, Self::Error>;

    /// Get the Arrow schema of a table.
    ///
    /// `catalog` or `db_schema` may be `None` when not applicable.
    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<Schema, Self::Error>;

    /// Get a list of table types in the database.
    ///
    /// The result is an Arrow dataset with the following schema:
    ///
    /// Field Name       | Field Type
    /// -----------------|--------------
    /// `table_type`     | `utf8 not null`
    fn get_table_types(&self) -> Result<Vec<String>, Self::Error>;

    /// Read part of a partitioned result set.
    fn read_partition(&self, partition: &[u8]) -> Result<Box<dyn RecordBatchReader>, Self::Error>;

    /// Commit any pending transactions. Only used if autocommit is disabled.
    fn commit(&self) -> Result<(), Self::Error>;

    /// Roll back any pending transactions. Only used if autocommit is disabled.
    fn rollback(&self) -> Result<(), Self::Error>;
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
pub trait StatementApi {
    type Error;

    /// Turn this statement into a prepared statement to be executed multiple times.
    ///
    /// This should return an error if called before [StatementApi::set_sql_query].
    fn prepare(&mut self) -> Result<(), Self::Error>;

    /// Set a string option on a statement.
    fn set_option(&mut self, key: &str, value: &str) -> Result<(), Self::Error>;

    /// Set the SQL query to execute.
    fn set_sql_query(&mut self, query: &str) -> Result<(), Self::Error>;

    /// Set the Substrait plan to execute.
    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<(), Self::Error>;

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
    /// This should return an error if this was called before [StatementApi::prepare].
    fn get_param_schema(&mut self) -> Result<Schema, Self::Error>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_data(&mut self, batch: RecordBatch) -> Result<(), Self::Error>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_stream(&mut self, stream: Box<dyn RecordBatchReader>) -> Result<(), Self::Error>;

    /// Execute a statement and get the results.
    ///
    /// See [StatementResult].
    fn execute(&mut self) -> Result<StatementResult, Self::Error>;

    /// Execute a query that doesn't have a result set.
    ///
    /// Will return the number of rows affected, or -1 if unknown or unsupported.
    fn execute_update(&mut self) -> Result<i64, Self::Error>;

    /// Execute a statement with a partitioned result set.
    ///
    /// This is not required to be implemented, as it only applies to backends
    /// that internally partition results. These backends can use this method
    /// to support threaded or distributed clients.
    ///
    /// See [PartitionedStatementResult].
    fn execute_partitioned(&mut self) -> Result<PartitionedStatementResult, Self::Error>;
}

/// Result of calling [StatementApi::execute].
///
/// `result` may be None if there is no meaningful result.
/// `row_affected` may be -1 if not applicable or if it is not supported.
pub struct StatementResult {
    pub result: Option<Box<dyn RecordBatchReader>>,
    pub rows_affected: i64,
}

/// Partitioned results
///
/// [ConnectionApi::read_partition] will be called to get the output stream
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

/// Structs and traits for representing database objects (tables, columns, schemas).
///
/// When [ConnectionApi::get_objects] is called, it returns an associated type that
/// implements [DatabaseCatalogCollection][objects::DatabaseCatalogCollection].
/// This collection contains a hierarchical data structure representing:
///
///  * Database catalogs
///  * Database schemas
///  * Tables
///  * Columns
///  * Table constraints
///
/// A database catalog, schema, and table are represented by a type implementing
/// [DatabaseCatalogEntry][objects::DatabaseCatalogEntry], [DatabaseSchemaEntry][objects::DatabaseSchemaEntry],
/// and [DatabaseTableEntry][objects::DatabaseTableEntry],
/// respectively. These can be concrete Rust structs, such as [SimpleCatalogEntry][objects::SimpleCatalogEntry],
/// [SimpleSchemaEntry][objects::SimpleSchemaEntry], and [SimpleTableEntry][objects::SimpleTableEntry].
/// Or they can be zero-copy views onto Arrow record batches as returned by the
/// C API ADBC drivers. The latter is implemented in
/// [ImportedCatalogCollection][crate::driver_manager::ImportedCatalogCollection].
///
/// | Trait                                       | Simple Rust-based                  | Arrow RecordBatch view |
/// |---------------------------------------------|---------------------------------------------------|---------|
/// | [DatabaseCatalogCollection][objects::DatabaseCatalogCollection] | [SimpleSchemaEntry][objects::SimpleCatalogCollection] | [ImportedCatalogCollection][crate::driver_manager::ImportedCatalogCollection] |
/// | [DatabaseCatalogEntry][objects::DatabaseCatalogEntry]       | [SimpleCatalogEntry][objects::SimpleCatalogEntry]       | [ImportedCatalogEntry][crate::driver_manager::ImportedCatalogEntry] |
/// | [DatabaseSchemaEntry][objects::DatabaseSchemaEntry] | [SimpleSchemaEntry][objects::SimpleSchemaEntry] | [ImportedSchemaEntry][crate::driver_manager::ImportedSchemaEntry] |
/// | [DatabaseTableEntry][objects::DatabaseTableEntry] | [SimpleTableEntry][objects::SimpleTableEntry] | [ImportedTableEntry][crate::driver_manager::ImportedTableEntry] |
///
/// There are owned and reference variations of columns, table constraints,
/// and foreign key usage. Each have a `borrow()` method to transform a owned
/// variant into its reference variant, and a `to_owned()` method to transform the
/// reference variant into the owned. These mimic the [std::borrow::Borrow] and
/// [std::borrow::ToOwned] traits, but do not actually implement them.
///
/// | Owned                                       | Reference                                         |
/// |---------------------------------------------|---------------------------------------------------|
/// | [ColumnSchema][objects::ColumnSchema]       | [ColumnSchemaRef][objects::ColumnSchemaRef]       |
/// | [TableConstraint][objects::TableConstraint] | [TableConstraintRef][objects::TableConstraintRef] |
/// | [ForeignKeyUsage][objects::ForeignKeyUsage] | [ForeignKeyUsageRef][objects::ForeignKeyUsageRef] |
pub mod objects {
    use super::*;

    /// A collection of database catalogs, returned by [ConnectionApi::get_objects].
    pub trait DatabaseCatalogCollection {
        type CatalogEntryType<'a>: DatabaseCatalogEntry<'a>
        where
            Self: 'a;

        /// List all catalogs in the result set.
        fn catalogs<'a>(&'a self) -> Box<dyn Iterator<Item = Self::CatalogEntryType<'a>> + 'a>;

        /// Get a particular catalog by name.
        ///
        /// Databases that have no notion of catalogs will have one with None for a name.
        /// This is case sensitive.
        fn get_catalog<'a>(&'a self, name: Option<&str>) -> Option<Self::CatalogEntryType<'a>> {
            self.catalogs().find(|catalog| catalog.name() == name)
        }

        /// Get as a record batch, as it would be exported in the C ADBC API.
        fn as_record_batch(&self) -> RecordBatch {
            todo!()
        }
    }

    /// An entry in a [DatabaseCatalogCollection] representing a single catalog.
    pub trait DatabaseCatalogEntry<'a> {
        type SchemaEntryType: DatabaseSchemaEntry<'a> + 'a;

        /// Get the name of the catalog.
        fn name(&self) -> Option<&'a str>;

        /// List all schemas in this catalog that are in the result set.
        fn schemas(&self) -> Box<dyn Iterator<Item = Self::SchemaEntryType> + 'a>;

        /// Get a particular schema by name.
        ///
        /// Databases that have no notion of schemas will have one with None for a name.
        /// This is case sensitive.
        fn get_schema(&self, name: Option<&str>) -> Option<Self::SchemaEntryType> {
            self.schemas().find(|schema| schema.name() == name)
        }
    }

    /// An entry in [DatabaseCatalogCollection] representing a single schema.
    pub trait DatabaseSchemaEntry<'a> {
        type TableEntryType: DatabaseTableEntry<'a>;

        /// Get the name of the schema.
        fn name(&self) -> Option<&'a str>;

        /// List all the tables in this schema that are in the result set.
        fn tables(&self) -> Box<dyn Iterator<Item = Self::TableEntryType> + 'a>;

        /// Get a particular table by name.
        ///
        /// This is case sensitive
        fn get_table(&self, name: &str) -> Option<Self::TableEntryType> {
            self.tables().find(|table| table.name() == name)
        }
    }

    /// An entry in the [DatabaseCatalogCollection] representing a single table.
    pub trait DatabaseTableEntry<'a> {
        /// The name of the table.
        fn name(&self) -> &'a str;

        /// The table type.
        ///
        /// Use [ConnectionApi::get_table_types] to get a list of supported types for
        /// the database.
        fn table_type(&self) -> &'a str;

        /// List all the columns in the table.
        fn columns(&self) -> Box<dyn Iterator<Item = ColumnSchemaRef<'a>> + 'a>;

        /// Get a column for a particular ordinal position.
        ///
        /// Will return None if the column is not found.
        fn get_column(&self, i: i32) -> Option<ColumnSchemaRef<'a>> {
            self.columns().find(|col| col.ordinal_position == i)
        }

        /// Get a column by name.
        ///
        /// This is case sensitive. Will return None if the column is not found.
        fn get_column_by_name(&self, name: &str) -> Option<ColumnSchemaRef<'a>> {
            self.columns().find(|col| col.name == name)
        }

        /// List all the constraints on the table.
        fn constraints(&self) -> Box<dyn Iterator<Item = TableConstraintRef<'a>> + 'a>;
    }

    /// A simple collection of database objects, made up of Rust data structures.
    pub struct SimpleCatalogCollection {
        catalogs: Vec<SimpleCatalogEntry>,
    }

    impl DatabaseCatalogCollection for SimpleCatalogCollection {
        type CatalogEntryType<'a> = &'a SimpleCatalogEntry;
        fn catalogs<'a>(&'a self) -> Box<dyn Iterator<Item = Self::CatalogEntryType<'a>> + 'a> {
            Box::new(self.catalogs.iter())
        }
    }

    /// A single database catalog. See [DatabaseCatalogEntry].
    pub struct SimpleCatalogEntry {
        name: Option<String>,
        db_schemas: Vec<SimpleSchemaEntry>,
    }

    impl<'a> DatabaseCatalogEntry<'a> for &'a SimpleCatalogEntry {
        type SchemaEntryType = &'a SimpleSchemaEntry;

        fn name(&self) -> Option<&'a str> {
            self.name.as_deref()
        }

        fn schemas(&self) -> Box<dyn Iterator<Item = Self::SchemaEntryType> + 'a> {
            Box::new(self.db_schemas.iter())
        }
    }

    /// A single database schema. See [DatabaseSchemaEntry].
    pub struct SimpleSchemaEntry {
        name: Option<String>,
        tables: Vec<SimpleTableEntry>,
    }

    impl<'a> DatabaseSchemaEntry<'a> for &'a SimpleSchemaEntry {
        type TableEntryType = &'a SimpleTableEntry;

        fn name(&self) -> Option<&'a str> {
            self.name.as_deref()
        }

        fn tables(&self) -> Box<dyn Iterator<Item = Self::TableEntryType> + 'a> {
            Box::new(self.tables.iter())
        }
    }

    /// A single table. See [DatabaseTableEntry].
    pub struct SimpleTableEntry {
        name: String,
        table_type: String,
        columns: Vec<ColumnSchema>,
        constraints: Vec<TableConstraint>,
    }

    impl<'a> DatabaseTableEntry<'a> for &'a SimpleTableEntry {
        fn name(&self) -> &'a str {
            &self.name
        }

        fn table_type(&self) -> &'a str {
            &self.table_type
        }

        fn columns(&self) -> Box<dyn Iterator<Item = ColumnSchemaRef<'a>> + 'a> {
            Box::new(self.columns.iter().map(|col| col.borrow()))
        }

        fn constraints(&self) -> Box<dyn Iterator<Item = TableConstraintRef<'a>> + 'a> {
            Box::new(
                self.constraints
                    .iter()
                    .map(|constraint| constraint.borrow()),
            )
        }
    }

    /// An entry in the [DatabaseCatalogCollection] representing a column.
    ///
    /// `xdbc_` columns are provided for compatibility with ODBC/JDBC column metadata.
    pub struct ColumnSchemaRef<'a> {
        /// The name of the column.
        pub name: &'a str,
        /// The ordinal position of the column.
        pub ordinal_position: i32,
        pub remarks: Option<&'a str>,
        pub xdbc_data_type: Option<i16>,
        pub xdbc_type_name: Option<&'a str>,
        pub xdbc_column_size: Option<i32>,
        pub xdbc_decimal_digits: Option<i16>,
        pub xdbc_num_prec_radix: Option<i16>,
        pub xdbc_nullable: Option<i16>,
        pub xdbc_column_def: Option<&'a str>,
        pub xdbc_sql_data_type: Option<i16>,
        pub xdbc_datetime_sub: Option<i16>,
        pub xdbc_char_octet_length: Option<i32>,
        pub xdbc_is_nullable: Option<&'a str>,
        pub xdbc_scope_catalog: Option<&'a str>,
        pub xdbc_scope_schema: Option<&'a str>,
        pub xdbc_scope_table: Option<&'a str>,
        pub xdbc_is_autoincrement: Option<bool>,
        pub xdbc_is_generatedcolumn: Option<bool>,
    }

    impl<'a> ColumnSchemaRef<'a> {
        pub fn to_owned(&self) -> ColumnSchema {
            ColumnSchema {
                name: self.name.to_owned(),
                ordinal_position: self.ordinal_position,
                remarks: self.remarks.as_ref().map(|&s| s.to_owned()),
                xdbc_data_type: self.xdbc_data_type,
                xdbc_type_name: self.xdbc_type_name.as_ref().map(|&s| s.to_owned()),
                xdbc_column_size: self.xdbc_column_size,
                xdbc_decimal_digits: self.xdbc_decimal_digits,
                xdbc_num_prec_radix: self.xdbc_num_prec_radix,
                xdbc_nullable: self.xdbc_nullable,
                xdbc_column_def: self.xdbc_column_def.as_ref().map(|&s| s.to_owned()),
                xdbc_sql_data_type: self.xdbc_sql_data_type,
                xdbc_datetime_sub: self.xdbc_datetime_sub,
                xdbc_char_octet_length: self.xdbc_char_octet_length,
                xdbc_is_nullable: self.xdbc_is_nullable.as_ref().map(|&s| s.to_owned()),
                xdbc_scope_catalog: self.xdbc_scope_catalog.as_ref().map(|&s| s.to_owned()),
                xdbc_scope_schema: self.xdbc_scope_schema.as_ref().map(|&s| s.to_owned()),
                xdbc_scope_table: self.xdbc_scope_table.as_ref().map(|&s| s.to_owned()),
                xdbc_is_autoincrement: self.xdbc_is_autoincrement,
                xdbc_is_generatedcolumn: self.xdbc_is_generatedcolumn,
            }
        }
    }

    /// An owning version of [ColumnSchema].
    pub struct ColumnSchema {
        name: String,
        ordinal_position: i32,
        remarks: Option<String>,
        xdbc_data_type: Option<i16>,
        xdbc_type_name: Option<String>,
        xdbc_column_size: Option<i32>,
        xdbc_decimal_digits: Option<i16>,
        xdbc_num_prec_radix: Option<i16>,
        xdbc_nullable: Option<i16>,
        xdbc_column_def: Option<String>,
        xdbc_sql_data_type: Option<i16>,
        xdbc_datetime_sub: Option<i16>,
        xdbc_char_octet_length: Option<i32>,
        xdbc_is_nullable: Option<String>,
        xdbc_scope_catalog: Option<String>,
        xdbc_scope_schema: Option<String>,
        xdbc_scope_table: Option<String>,
        xdbc_is_autoincrement: Option<bool>,
        xdbc_is_generatedcolumn: Option<bool>,
    }

    impl ColumnSchema {
        pub fn borrow(&self) -> ColumnSchemaRef<'_> {
            ColumnSchemaRef {
                name: &self.name,
                ordinal_position: self.ordinal_position,
                remarks: self.remarks.as_deref(),
                xdbc_data_type: self.xdbc_data_type,
                xdbc_type_name: self.xdbc_type_name.as_deref(),
                xdbc_column_size: self.xdbc_column_size,
                xdbc_decimal_digits: self.xdbc_decimal_digits,
                xdbc_num_prec_radix: self.xdbc_num_prec_radix,
                xdbc_nullable: self.xdbc_nullable,
                xdbc_column_def: self.xdbc_column_def.as_deref(),
                xdbc_sql_data_type: self.xdbc_sql_data_type,
                xdbc_datetime_sub: self.xdbc_datetime_sub,
                xdbc_char_octet_length: self.xdbc_char_octet_length,
                xdbc_is_nullable: self.xdbc_is_nullable.as_deref(),
                xdbc_scope_catalog: self.xdbc_scope_catalog.as_deref(),
                xdbc_scope_schema: self.xdbc_scope_schema.as_deref(),
                xdbc_scope_table: self.xdbc_scope_table.as_deref(),
                xdbc_is_autoincrement: self.xdbc_is_autoincrement,
                xdbc_is_generatedcolumn: self.xdbc_is_generatedcolumn,
            }
        }
    }

    /// An entry in the [DatabaseCatalogCollection] representing a table constraint.
    pub enum TableConstraintRef<'a> {
        Check {
            name: Option<&'a str>,
            columns: Vec<&'a str>,
        },
        ForeignKey {
            name: Option<&'a str>,
            columns: Vec<&'a str>,
            usage: Vec<ForeignKeyUsageRef<'a>>,
        },
        PrimaryKey {
            name: Option<&'a str>,
            columns: Vec<&'a str>,
        },
        Unique {
            name: Option<&'a str>,
            columns: Vec<&'a str>,
        },
    }

    impl<'a> TableConstraintRef<'a> {
        pub fn to_owned(&self) -> TableConstraint {
            match self {
                Self::Check { name, columns } => TableConstraint::Check {
                    name: name.as_ref().map(|&s| s.to_owned()),
                    columns: columns.iter().map(|&s| s.to_owned()).collect(),
                },
                Self::PrimaryKey { name, columns } => TableConstraint::PrimaryKey {
                    name: name.as_ref().map(|&s| s.to_owned()),
                    columns: columns.iter().map(|&s| s.to_owned()).collect(),
                },
                Self::Unique { name, columns } => TableConstraint::Unique {
                    name: name.as_ref().map(|&s| s.to_owned()),
                    columns: columns.iter().map(|&s| s.to_owned()).collect(),
                },
                Self::ForeignKey {
                    name,
                    columns,
                    usage,
                } => TableConstraint::ForeignKey {
                    name: name.as_ref().map(|&s| s.to_owned()),
                    columns: columns.iter().map(|&s| s.to_owned()).collect(),
                    usage: usage.iter().map(|u| u.to_owned()).collect(),
                },
            }
        }
    }

    /// The location of a foreign key. Used in [TableConstraint].
    pub struct ForeignKeyUsageRef<'a> {
        pub catalog: Option<&'a str>,
        pub db_schema: Option<&'a str>,
        pub table: &'a str,
        pub column_name: &'a str,
    }

    impl<'a> ForeignKeyUsageRef<'a> {
        pub fn to_owned(&self) -> ForeignKeyUsage {
            ForeignKeyUsage {
                catalog: self.catalog.as_ref().map(|&s| s.to_owned()),
                db_schema: self.db_schema.as_ref().map(|&s| s.to_owned()),
                table: self.table.to_owned(),
                column_name: self.column_name.to_owned(),
            }
        }
    }

    /// An owning version of [TableConstraintRef].
    pub enum TableConstraint {
        Check {
            name: Option<String>,
            columns: Vec<String>,
        },
        ForeignKey {
            name: Option<String>,
            columns: Vec<String>,
            usage: Vec<ForeignKeyUsage>,
        },
        PrimaryKey {
            name: Option<String>,
            columns: Vec<String>,
        },
        Unique {
            name: Option<String>,
            columns: Vec<String>,
        },
    }

    impl TableConstraint {
        pub fn borrow(&self) -> TableConstraintRef<'_> {
            match self {
                Self::Check { name, columns } => TableConstraintRef::Check {
                    name: name.as_deref(),
                    columns: columns.iter().map(|s| s.as_str()).collect(),
                },
                Self::PrimaryKey { name, columns } => TableConstraintRef::PrimaryKey {
                    name: name.as_deref(),
                    columns: columns.iter().map(|s| s.as_str()).collect(),
                },
                Self::Unique { name, columns } => TableConstraintRef::Unique {
                    name: name.as_deref(),
                    columns: columns.iter().map(|s| s.as_str()).collect(),
                },
                Self::ForeignKey {
                    name,
                    columns,
                    usage,
                } => TableConstraintRef::ForeignKey {
                    name: name.as_deref(),
                    columns: columns.iter().map(|s| s.as_str()).collect(),
                    usage: usage.iter().map(|u| u.borrow()).collect(),
                },
            }
        }
    }

    /// An owning version of [ForeignKeyUsageRef].
    pub struct ForeignKeyUsage {
        pub catalog: Option<String>,
        pub db_schema: Option<String>,
        pub table: String,
        pub column_name: String,
    }

    impl ForeignKeyUsage {
        pub fn borrow(&self) -> ForeignKeyUsageRef<'_> {
            ForeignKeyUsageRef {
                catalog: self.catalog.as_deref(),
                db_schema: self.db_schema.as_deref(),
                table: &self.table,
                column_name: &self.column_name,
            }
        }
    }
}
