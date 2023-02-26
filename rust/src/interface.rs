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

use crate::error::AdbcError;

/// Databases hold state shared by multiple connections. This typically means
/// configuration and caches. For in-memory databases, it provides a place to
/// hold ownership of the in-memory database.
pub trait DatabaseApi {
    /// Set an option on the database.
    ///
    /// Some databases may not allow setting options after it has been initialized.
    fn set_option(&self, key: &str, value: &str) -> Result<(), AdbcError>;
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
    type ObjectCollectionType: objects::DatabaseCatalogCollection;

    /// Set an option on the connection.
    ///
    /// Some connections may not allow setting options after it has been initialized.
    fn set_option(&self, key: &str, value: &str) -> Result<(), AdbcError>;

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
    fn get_info(&self, info_codes: Option<&[u32]>) -> Result<Vec<(u32, InfoData)>, AdbcError>;

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
    fn read_partition(&self, partition: &[u8]) -> Result<Box<dyn RecordBatchReader>, AdbcError>;

    /// Commit any pending transactions. Only used if autocommit is disabled.
    fn commit(&self) -> Result<(), AdbcError>;

    /// Roll back any pending transactions. Only used if autocommit is disabled.
    fn rollback(&self) -> Result<(), AdbcError>;
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
    /// Turn this statement into a prepared statement to be executed multiple time.
    ///
    /// This should return an error if called before [StatementApi::set_sql_query].
    fn prepare(&mut self) -> Result<(), AdbcError>;

    /// Set a string option on a statement.
    fn set_option(&mut self, key: &str, value: &str) -> Result<(), AdbcError>;

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
    /// This should return an error if this was called before [StatementApi::prepare].
    fn get_param_schema(&mut self) -> Result<Schema, AdbcError>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_data(&mut self, batch: RecordBatch) -> Result<(), AdbcError>;

    /// Bind Arrow data, either for bulk inserts or prepared statements.
    fn bind_stream(&mut self, stream: Box<dyn RecordBatchReader>) -> Result<(), AdbcError>;

    /// Execute a statement and get the results.
    ///
    /// See [StatementResult].
    fn execute(&mut self) -> Result<StatementResult, AdbcError>;

    /// Execute a query that doesn't have a result set.
    ///
    /// Will return the number of rows affected, or -1 if unknown or unsupported.
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
    use std::sync::Arc;

    use arrow::{
        array::{
            Array, ArrayBuilder, ArrayDataBuilder, ArrayRef, BooleanBufferBuilder, BooleanBuilder,
            Int16Builder, Int32BufferBuilder, Int32Builder, ListArray, ListBuilder, StringBuilder,
            StructArray,
        },
        datatypes::{DataType, Field},
    };

    use super::*;

    pub(crate) struct UsageArrayBuilder {
        fk_catalog: StringBuilder,
        fk_db_schema: StringBuilder,
        fk_table: StringBuilder,
        fk_column_name: StringBuilder,
    }

    impl UsageArrayBuilder {
        pub fn with_capacity(catalogs: &impl DatabaseCatalogCollection) -> Self {
            let constraints = catalogs
                .catalogs()
                .flat_map(|catalog| catalog.schemas())
                .flat_map(|schema| schema.tables())
                .flat_map(|table| table.constraints());
            let mut num_usages = 0;
            let mut catalog_len = 0;
            let mut db_schema_len = 0;
            let mut table_len = 0;
            let mut column_len = 0;

            for constraint in constraints {
                if let TableConstraintTypeRef::ForeignKey { usage } = constraint.constraint_type {
                    num_usages += usage.len();

                    for entry in usage {
                        catalog_len += entry.catalog.map(|s| s.len()).unwrap_or_default();
                        db_schema_len += entry.db_schema.map(|s| s.len()).unwrap_or_default();
                        table_len += entry.table.len();
                        column_len += entry.column_name.len();
                    }
                }
            }

            Self {
                fk_catalog: StringBuilder::with_capacity(num_usages, catalog_len),
                fk_db_schema: StringBuilder::with_capacity(num_usages, db_schema_len),
                fk_table: StringBuilder::with_capacity(num_usages, table_len),
                fk_column_name: StringBuilder::with_capacity(num_usages, column_len),
            }
        }

        pub fn schema() -> Vec<Field> {
            vec![
                Field::new("fk_catalog", DataType::Utf8, true),
                Field::new("fk_db_schema", DataType::Utf8, true),
                Field::new("fk_table", DataType::Utf8, false),
                Field::new("fk_column_name", DataType::Utf8, false),
            ]
        }

        pub fn append_usages(&mut self, usages: &[ForeignKeyUsageRef]) {
            for usage in usages {
                self.fk_catalog.append_option(usage.catalog);
                self.fk_db_schema.append_option(usage.db_schema);
                self.fk_table.append_value(usage.table);
                self.fk_column_name.append_value(usage.column_name);
            }
        }

        pub fn current_offset(&self) -> i32 {
            self.fk_table
                .len()
                .try_into()
                .expect("i32 overflow in tables offset.")
        }

        pub fn finish(mut self) -> StructArray {
            let arrays: &[ArrayRef; 4] = &[
                Arc::new(self.fk_catalog.finish()),
                Arc::new(self.fk_db_schema.finish()),
                Arc::new(self.fk_table.finish()),
                Arc::new(self.fk_column_name.finish()),
            ];

            StructArray::from(
                Self::schema()
                    .into_iter()
                    .zip(arrays.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub(crate) struct ConstraintArrayBuilder {
        constraint_name: StringBuilder,
        constraint_type: StringBuilder,
        constraint_cols: ListBuilder<StringBuilder>,
        usage: UsageArrayBuilder,
        usage_offsets: Int32BufferBuilder,
        usage_validity: BooleanBufferBuilder,
    }

    impl ConstraintArrayBuilder {
        pub fn with_capacity(catalogs: &impl DatabaseCatalogCollection) -> Self {
            let constraints = catalogs
                .catalogs()
                .flat_map(|catalog| catalog.schemas())
                .flat_map(|schema| schema.tables())
                .flat_map(|table| table.constraints());
            let mut num_constraints = 0;
            let mut name_len = 0;
            let mut type_len = 0;
            let mut num_cols = 0;
            let mut cols_len = 0;

            for constraint in constraints {
                num_constraints += 1;
                name_len += constraint.name.map(|s| s.len()).unwrap_or_default();
                type_len += constraint.constraint_type.variant_name().len();
                num_cols += constraint.columns.len();
                cols_len += constraint.columns.iter().map(|s| s.len()).sum::<usize>();
            }

            let constraint_cols = ListBuilder::with_capacity(
                StringBuilder::with_capacity(num_cols, cols_len),
                num_constraints,
            );

            // Start with first offset;
            let mut usage_offsets = Int32BufferBuilder::new(num_constraints + 1);
            usage_offsets.append(0);

            Self {
                constraint_name: StringBuilder::with_capacity(num_constraints, name_len),
                constraint_type: StringBuilder::with_capacity(num_constraints, type_len),
                constraint_cols,
                usage: UsageArrayBuilder::with_capacity(catalogs),
                usage_offsets,
                usage_validity: BooleanBufferBuilder::new(num_constraints),
            }
        }

        pub fn schema() -> Vec<Field> {
            let usage_schema = DataType::Struct(UsageArrayBuilder::schema());
            vec![
                Field::new("constraint_name", DataType::Utf8, true),
                Field::new("constraint_type", DataType::Utf8, false),
                Field::new(
                    "constraint_column_names",
                    DataType::List(Box::new(Field::new("item", DataType::Utf8, true))),
                    false,
                ),
                Field::new(
                    "constraint_column_usage",
                    DataType::List(Box::new(Field::new("item", usage_schema, true))),
                    true,
                ),
            ]
        }

        pub fn append_constraints<'a>(
            &mut self,
            constraints: impl Iterator<Item = TableConstraintRef<'a>>,
        ) {
            for constraint in constraints {
                self.constraint_name.append_option(constraint.name);
                self.constraint_type
                    .append_value(constraint.constraint_type.variant_name());
                for col_name in constraint.columns {
                    self.constraint_cols.values().append_value(col_name);
                }
                self.constraint_cols.append(true);
                match constraint.constraint_type {
                    TableConstraintTypeRef::ForeignKey { usage } => {
                        self.usage.append_usages(&usage);
                        self.usage_offsets.append(self.usage.current_offset());
                        self.usage_validity.append(true);
                    }
                    _ => {
                        self.usage_offsets.append(self.usage.current_offset());
                        self.usage_validity.append(false);
                    }
                }
            }
        }

        pub fn current_offset(&self) -> i32 {
            self.constraint_name
                .len()
                .try_into()
                .expect("i32 overflow for constraints")
        }

        pub fn finish(mut self) -> StructArray {
            let usage_data = ArrayDataBuilder::new(Self::schema()[3].data_type().clone())
                .null_bit_buffer(Some(self.usage_validity.finish()))
                .add_child_data(self.usage.finish().data().clone())
                .add_buffer(self.usage_offsets.finish())
                .build()
                .expect("usage data is invalid");

            let arrays: &[ArrayRef; 4] = &[
                Arc::new(self.constraint_name.finish()),
                Arc::new(self.constraint_type.finish()),
                Arc::new(self.constraint_cols.finish()),
                Arc::new(ListArray::from(usage_data)),
            ];

            StructArray::from(
                Self::schema()
                    .into_iter()
                    .zip(arrays.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub(crate) struct ColumnArrayBuilder {
        column_name: StringBuilder,
        ordinal_position: Int32Builder,
        remarks: StringBuilder,
        xdbc_data_type: Int16Builder,
        xdbc_type_name: StringBuilder,
        xdbc_column_size: Int32Builder,
        xdbc_decimal_digits: Int16Builder,
        xdbc_num_prec_radix: Int16Builder,
        xdbc_nullable: Int16Builder,
        xdbc_column_def: StringBuilder,
        xdbc_sql_data_type: Int16Builder,
        xdbc_datetime_sub: Int16Builder,
        xdbc_char_octet_length: Int32Builder,
        xdbc_is_nullable: StringBuilder,
        xdbc_scope_catalog: StringBuilder,
        xdbc_scope_schema: StringBuilder,
        xdbc_scope_table: StringBuilder,
        xdbc_is_autoincrement: BooleanBuilder,
        xdbc_is_generatedcolumn: BooleanBuilder,
    }

    impl ColumnArrayBuilder {
        pub fn with_capacity(catalogs: &impl DatabaseCatalogCollection) -> Self {
            let columns = catalogs
                .catalogs()
                .flat_map(|catalog| catalog.schemas())
                .flat_map(|schema| schema.tables())
                .flat_map(|table| table.columns());

            let mut num_columns = 0;
            let mut name_len = 0;
            let mut remarks_len = 0;
            let mut xdbc_type_name_len = 0;
            let mut xdbc_column_def_len = 0;
            let mut xdbc_is_nullable_len = 0;
            let mut xdbc_scope_catalog_len = 0;
            let mut xdbc_scope_schema_len = 0;
            let mut xdbc_scope_table_len = 0;

            for column in columns {
                num_columns += 1;
                name_len += column.name.len();
                remarks_len += column.remarks.map(|s| s.len()).unwrap_or_default();
                xdbc_type_name_len = column.xdbc_type_name.map(|s| s.len()).unwrap_or_default();
                xdbc_column_def_len = column.xdbc_column_def.map(|s| s.len()).unwrap_or_default();
                xdbc_is_nullable_len = column.xdbc_is_nullable.map(|s| s.len()).unwrap_or_default();
                xdbc_scope_catalog_len = column
                    .xdbc_scope_catalog
                    .map(|s| s.len())
                    .unwrap_or_default();
                xdbc_scope_schema_len = column
                    .xdbc_scope_schema
                    .map(|s| s.len())
                    .unwrap_or_default();
                xdbc_scope_table_len = column.xdbc_scope_table.map(|s| s.len()).unwrap_or_default();
            }

            Self {
                column_name: StringBuilder::with_capacity(num_columns, name_len),
                ordinal_position: Int32Builder::with_capacity(num_columns),
                remarks: StringBuilder::with_capacity(num_columns, remarks_len),
                xdbc_data_type: Int16Builder::with_capacity(num_columns),
                xdbc_type_name: StringBuilder::with_capacity(num_columns, xdbc_type_name_len),
                xdbc_column_size: Int32Builder::with_capacity(num_columns),
                xdbc_decimal_digits: Int16Builder::with_capacity(num_columns),
                xdbc_num_prec_radix: Int16Builder::with_capacity(num_columns),
                xdbc_nullable: Int16Builder::with_capacity(num_columns),
                xdbc_column_def: StringBuilder::with_capacity(num_columns, xdbc_column_def_len),
                xdbc_sql_data_type: Int16Builder::with_capacity(num_columns),
                xdbc_datetime_sub: Int16Builder::with_capacity(num_columns),
                xdbc_char_octet_length: Int32Builder::with_capacity(num_columns),
                xdbc_is_nullable: StringBuilder::with_capacity(num_columns, xdbc_is_nullable_len),
                xdbc_scope_catalog: StringBuilder::with_capacity(
                    num_columns,
                    xdbc_scope_catalog_len,
                ),
                xdbc_scope_schema: StringBuilder::with_capacity(num_columns, xdbc_scope_schema_len),
                xdbc_scope_table: StringBuilder::with_capacity(num_columns, xdbc_scope_table_len),
                xdbc_is_autoincrement: BooleanBuilder::with_capacity(num_columns),
                xdbc_is_generatedcolumn: BooleanBuilder::with_capacity(num_columns),
            }
        }

        pub fn schema() -> Vec<Field> {
            vec![
                Field::new("column_name", DataType::Utf8, false),
                Field::new("ordinal_position", DataType::Int32, true),
                Field::new("remarks", DataType::Utf8, true),
                Field::new("xdbc_data_type", DataType::Int16, true),
                Field::new("xdbc_type_name", DataType::Utf8, true),
                Field::new("xdbc_column_size", DataType::Int32, true),
                Field::new("xdbc_decimal_digits", DataType::Int16, true),
                Field::new("xdbc_num_prec_radix", DataType::Int16, true),
                Field::new("xdbc_nullable", DataType::Int16, true),
                Field::new("xdbc_column_def", DataType::Utf8, true),
                Field::new("xdbc_sql_data_type", DataType::Int16, true),
                Field::new("xdbc_datetime_sub", DataType::Int16, true),
                Field::new("xdbc_char_octet_length", DataType::Int32, true),
                Field::new("xdbc_is_nullable", DataType::Utf8, true),
                Field::new("xdbc_scope_catalog", DataType::Utf8, true),
                Field::new("xdbc_scope_schema", DataType::Utf8, true),
                Field::new("xdbc_scope_table", DataType::Utf8, true),
                Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
                Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
            ]
        }

        pub fn append_columns<'a>(&mut self, columns: impl Iterator<Item = ColumnSchemaRef<'a>>) {
            for column in columns {
                self.column_name.append_value(column.name);
                self.ordinal_position.append_value(column.ordinal_position);
                self.remarks.append_option(column.remarks);
                self.xdbc_data_type.append_option(column.xdbc_data_type);
                self.xdbc_type_name.append_option(column.xdbc_type_name);
                self.xdbc_column_size.append_option(column.xdbc_column_size);
                self.xdbc_decimal_digits
                    .append_option(column.xdbc_decimal_digits);
                self.xdbc_num_prec_radix
                    .append_option(column.xdbc_num_prec_radix);
                self.xdbc_nullable.append_option(column.xdbc_nullable);
                self.xdbc_column_def.append_option(column.xdbc_column_def);
                self.xdbc_sql_data_type
                    .append_option(column.xdbc_sql_data_type);
                self.xdbc_datetime_sub
                    .append_option(column.xdbc_datetime_sub);
                self.xdbc_char_octet_length
                    .append_option(column.xdbc_char_octet_length);
                self.xdbc_is_nullable.append_option(column.xdbc_is_nullable);
                self.xdbc_scope_catalog
                    .append_option(column.xdbc_scope_catalog);
                self.xdbc_scope_schema
                    .append_option(column.xdbc_scope_schema);
                self.xdbc_scope_table.append_option(column.xdbc_scope_table);
                self.xdbc_is_autoincrement
                    .append_option(column.xdbc_is_autoincrement);
                self.xdbc_is_generatedcolumn
                    .append_option(column.xdbc_is_generatedcolumn);
            }
        }

        pub fn current_offset(&self) -> i32 {
            self.column_name
                .len()
                .try_into()
                .expect("i32 overflow for number of columns")
        }

        pub fn finish(mut self) -> StructArray {
            let arrays: &[ArrayRef; 19] = &[
                Arc::new(self.column_name.finish()),
                Arc::new(self.ordinal_position.finish()),
                Arc::new(self.remarks.finish()),
                Arc::new(self.xdbc_data_type.finish()),
                Arc::new(self.xdbc_type_name.finish()),
                Arc::new(self.xdbc_column_size.finish()),
                Arc::new(self.xdbc_decimal_digits.finish()),
                Arc::new(self.xdbc_num_prec_radix.finish()),
                Arc::new(self.xdbc_nullable.finish()),
                Arc::new(self.xdbc_column_def.finish()),
                Arc::new(self.xdbc_sql_data_type.finish()),
                Arc::new(self.xdbc_datetime_sub.finish()),
                Arc::new(self.xdbc_char_octet_length.finish()),
                Arc::new(self.xdbc_is_nullable.finish()),
                Arc::new(self.xdbc_scope_catalog.finish()),
                Arc::new(self.xdbc_scope_schema.finish()),
                Arc::new(self.xdbc_scope_table.finish()),
                Arc::new(self.xdbc_is_autoincrement.finish()),
                Arc::new(self.xdbc_is_generatedcolumn.finish()),
            ];
            StructArray::from(
                Self::schema()
                    .into_iter()
                    .zip(arrays.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub(crate) struct TableArrayBuilder {
        table_name: StringBuilder,
        table_type: StringBuilder,
        table_columns: ColumnArrayBuilder,
        columns_offsets: Int32BufferBuilder,
        // TODO: it would be nice if NullBufferBuilder was public.
        columns_validity: BooleanBufferBuilder,
        table_constraints: ConstraintArrayBuilder,
        constraints_offsets: Int32BufferBuilder,
        constraints_validity: BooleanBufferBuilder,
    }

    impl TableArrayBuilder {
        pub fn with_capacity<T: DatabaseCatalogCollection>(catalogs: &T) -> Self {
            let tables = catalogs
                .catalogs()
                .flat_map(|catalog| catalog.schemas())
                .flat_map(|schema| schema.tables());

            let mut num_tables = 0;
            let mut name_len = 0;
            let mut type_len = 0;

            for table in tables {
                num_tables += 1;
                name_len += table.name().len();
                type_len += table.table_type().len();
            }

            let mut columns_offsets = Int32BufferBuilder::new(num_tables);
            columns_offsets.append(0);

            let mut constraints_offsets = Int32BufferBuilder::new(num_tables);
            constraints_offsets.append(0);

            Self {
                table_name: StringBuilder::with_capacity(num_tables, name_len),
                table_type: StringBuilder::with_capacity(num_tables, type_len),
                table_columns: ColumnArrayBuilder::with_capacity(catalogs),
                columns_offsets,
                columns_validity: BooleanBufferBuilder::new(num_tables),
                table_constraints: ConstraintArrayBuilder::with_capacity(catalogs),
                constraints_offsets,
                constraints_validity: BooleanBufferBuilder::new(num_tables),
            }
        }

        pub fn schema() -> Vec<Field> {
            let constraint_schema = DataType::Struct(ConstraintArrayBuilder::schema());
            let column_schema = DataType::Struct(ColumnArrayBuilder::schema());

            vec![
                Field::new("table_name", DataType::Utf8, false),
                Field::new("table_type", DataType::Utf8, false),
                Field::new(
                    "table_columns",
                    DataType::List(Box::new(Field::new("item", column_schema, true))),
                    true,
                ),
                Field::new(
                    "table_constraints",
                    DataType::List(Box::new(Field::new("item", constraint_schema, true))),
                    true,
                ),
            ]
        }

        pub fn append_tables<'a>(
            &mut self,
            tables: impl Iterator<Item = impl DatabaseTableEntry<'a>>,
        ) {
            for table in tables {
                self.table_name.append_value(table.name());
                self.table_type.append_value(table.table_type());

                self.table_columns.append_columns(table.columns());
                self.columns_offsets
                    .append(self.table_columns.current_offset());
                self.columns_validity.append(true);

                self.table_constraints
                    .append_constraints(table.constraints());
                self.constraints_offsets
                    .append(self.table_constraints.current_offset());
                self.constraints_validity.append(true);
            }
        }

        pub fn current_offset(&self) -> i32 {
            self.table_name
                .len()
                .try_into()
                .expect("i32 overflow for tables")
        }

        pub fn finish(mut self) -> StructArray {
            let columns_data = ArrayDataBuilder::new(Self::schema()[3].data_type().clone())
                .null_bit_buffer(Some(self.columns_validity.finish()))
                .add_child_data(self.table_columns.finish().data().clone())
                .add_buffer(self.columns_offsets.finish())
                .build()
                .expect("columns data is invalid");

            let constraints_data = ArrayDataBuilder::new(Self::schema()[3].data_type().clone())
                .null_bit_buffer(Some(self.constraints_validity.finish()))
                .add_child_data(self.table_constraints.finish().data().clone())
                .add_buffer(self.constraints_offsets.finish())
                .build()
                .expect("constraints data is invalid");

            let arrays: &[ArrayRef; 4] = &[
                Arc::new(self.table_name.finish()),
                Arc::new(self.table_type.finish()),
                Arc::new(ListArray::from(columns_data)),
                Arc::new(ListArray::from(constraints_data)),
            ];

            StructArray::from(
                Self::schema()
                    .into_iter()
                    .zip(arrays.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub(crate) struct DbSchemaArrayBuilder {
        db_schema_name: StringBuilder,
        tables: TableArrayBuilder,
        tables_offsets: Int32BufferBuilder,
        tables_validity: BooleanBufferBuilder,
    }

    impl DbSchemaArrayBuilder {
        pub fn with_capacity(catalogs: &impl DatabaseCatalogCollection) -> Self {
            let schemas = catalogs.catalogs().flat_map(|catalog| catalog.schemas());

            let mut num_schemas = 0;
            let mut name_len = 0;

            for schema in schemas {
                num_schemas += 1;
                name_len += schema.name().map(|s| s.len()).unwrap_or_default();
            }

            let mut tables_offsets = Int32BufferBuilder::new(num_schemas);
            tables_offsets.append(0);

            Self {
                db_schema_name: StringBuilder::with_capacity(num_schemas, name_len),
                tables: TableArrayBuilder::with_capacity(catalogs),
                tables_offsets,
                tables_validity: BooleanBufferBuilder::new(num_schemas),
            }
        }

        pub fn schema() -> Vec<Field> {
            let table_schema = DataType::Struct(TableArrayBuilder::schema());

            vec![
                Field::new("db_schema_name", DataType::Utf8, true),
                Field::new(
                    "db_schema_tables",
                    DataType::List(Box::new(Field::new("item", table_schema, true))),
                    true,
                ),
            ]
        }

        pub fn append_schemas<'a>(
            &mut self,
            schemas: impl Iterator<Item = impl DatabaseSchemaEntry<'a>>,
        ) {
            for schema in schemas {
                self.db_schema_name.append_option(schema.name());
                self.tables.append_tables(schema.tables());
                self.tables_offsets.append(self.tables.current_offset());
                self.tables_validity.append(true);
            }
        }

        pub fn current_offset(&self) -> i32 {
            self.db_schema_name
                .len()
                .try_into()
                .expect("i32 overflow in db schemas")
        }

        pub fn finish(mut self) -> StructArray {
            let tables_data = ArrayDataBuilder::new(Self::schema()[3].data_type().clone())
                .null_bit_buffer(Some(self.tables_validity.finish()))
                .add_child_data(self.tables.finish().data().clone())
                .add_buffer(self.tables_offsets.finish())
                .build()
                .expect("columns data is invalid");

            let arrays: &[ArrayRef; 2] = &[
                Arc::new(self.db_schema_name.finish()),
                Arc::new(ListArray::from(tables_data)),
            ];

            StructArray::from(
                Self::schema()
                    .into_iter()
                    .zip(arrays.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        }
    }

    pub(crate) struct CatalogArrayBuilder {
        catalog_name: StringBuilder,
        schemas: DbSchemaArrayBuilder,
        schemas_offsets: Int32BufferBuilder,
        schemas_validity: BooleanBufferBuilder,
    }

    impl CatalogArrayBuilder {
        pub fn with_capacity<T: DatabaseCatalogCollection>(collection: &T) -> Self {
            let catalogs = collection.catalogs();

            let mut num_catalogs = 0;
            let mut name_len = 0;

            for catalog in catalogs {
                num_catalogs += 1;
                name_len += catalog.name().map(|s| s.len()).unwrap_or_default();
            }

            let mut schemas_offsets = Int32BufferBuilder::new(num_catalogs);
            schemas_offsets.append(0);

            Self {
                catalog_name: StringBuilder::with_capacity(num_catalogs, name_len),
                schemas: DbSchemaArrayBuilder::with_capacity(collection),
                schemas_offsets,
                schemas_validity: BooleanBufferBuilder::new(num_catalogs),
            }
        }

        pub fn schema() -> Vec<Field> {
            let db_schema_schema = DataType::Struct(DbSchemaArrayBuilder::schema());

            vec![
                Field::new("catalog_name", DataType::Utf8, true),
                Field::new(
                    "catalog_db_schemas",
                    DataType::List(Box::new(Field::new("item", db_schema_schema, true))),
                    true,
                ),
            ]
        }

        pub fn append_catalogs<'a>(
            &mut self,
            catalogs: impl Iterator<Item = impl DatabaseCatalogEntry<'a>>,
        ) {
            for catalog in catalogs {
                self.catalog_name.append_option(catalog.name());
                self.schemas.append_schemas(catalog.schemas());
                self.schemas_offsets.append(self.schemas.current_offset());
                self.schemas_validity.append(true);
            }
        }

        pub fn finish(mut self) -> StructArray {
            let schemas_data = ArrayDataBuilder::new(Self::schema()[3].data_type().clone())
                .null_bit_buffer(Some(self.schemas_validity.finish()))
                .add_child_data(self.schemas.finish().data().clone())
                .add_buffer(self.schemas_offsets.finish())
                .build()
                .expect("columns data is invalid");

            let arrays: &[ArrayRef; 2] = &[
                Arc::new(self.catalog_name.finish()),
                Arc::new(ListArray::from(schemas_data)),
            ];

            StructArray::from(
                Self::schema()
                    .into_iter()
                    .zip(arrays.iter().cloned())
                    .collect::<Vec<_>>(),
            )
        }
    }

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

        /// Get as a record batch, for exporting in the C ADBC API.
        fn as_record_batch(&self) -> RecordBatch
        where
            Self: Sized,
        {
            let mut builder = CatalogArrayBuilder::with_capacity(self);
            builder.append_catalogs(self.catalogs());
            let array = builder.finish();
            RecordBatch::from(&array)
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
    pub struct TableConstraintRef<'a> {
        pub name: Option<&'a str>,
        pub columns: Vec<&'a str>,
        pub constraint_type: TableConstraintTypeRef<'a>,
    }

    pub enum TableConstraintTypeRef<'a> {
        Check,
        PrimaryKey,
        ForeignKey { usage: Vec<ForeignKeyUsageRef<'a>> },
        Unique,
    }

    impl<'a> TableConstraintRef<'a> {
        pub fn to_owned(&self) -> TableConstraint {
            let name = self.name.as_ref().map(|&s| s.to_owned());
            let columns = self.columns.iter().map(|&s| s.to_owned()).collect();

            let constraint_type = match &self.constraint_type {
                TableConstraintTypeRef::ForeignKey { usage } => TableConstraintType::ForeignKey {
                    usage: usage.iter().map(|u| u.to_owned()).collect(),
                },
                TableConstraintTypeRef::Check => TableConstraintType::Check,
                TableConstraintTypeRef::PrimaryKey => TableConstraintType::PrimaryKey,
                TableConstraintTypeRef::Unique => TableConstraintType::Unique,
            };

            TableConstraint {
                name,
                columns,
                constraint_type,
            }
        }
    }

    impl<'a> TableConstraintTypeRef<'a> {
        pub fn variant_name(&self) -> &'static str {
            match self {
                Self::Check => "CHECK",
                Self::ForeignKey { usage: _ } => "FOREIGN_KEY",
                Self::PrimaryKey => "PRIMARY_KEY",
                Self::Unique => "UNIQUE",
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
    pub struct TableConstraint {
        name: Option<String>,
        columns: Vec<String>,
        constraint_type: TableConstraintType,
    }

    pub enum TableConstraintType {
        Check,
        PrimaryKey,
        ForeignKey { usage: Vec<ForeignKeyUsage> },
        Unique,
    }

    impl TableConstraint {
        pub fn borrow(&self) -> TableConstraintRef<'_> {
            let name = self.name.as_deref();
            let columns = self.columns.iter().map(|s| s.as_str()).collect();

            let constraint_type = match &self.constraint_type {
                TableConstraintType::ForeignKey { usage } => TableConstraintTypeRef::ForeignKey {
                    usage: usage.iter().map(|u| u.borrow()).collect(),
                },
                TableConstraintType::Check => TableConstraintTypeRef::Check,
                TableConstraintType::PrimaryKey => TableConstraintTypeRef::PrimaryKey,
                TableConstraintType::Unique => TableConstraintTypeRef::Unique,
            };

            TableConstraintRef {
                name,
                columns,
                constraint_type,
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
