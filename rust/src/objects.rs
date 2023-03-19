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

//! Structs and traits for representing database objects (tables, columns, schemas).
//!
//! When [crate::AdbcConnection::get_objects] is called, it returns an associated type that
//! implements [DatabaseCatalogCollection]. This collection contains a hierarchical data
//! structure representing:
//!
//!  * Database catalogs
//!  * Database schemas
//!  * Tables
//!  * Columns
//!  * Table constraints
//!
//! A database catalog, schema, and table are represented by a type implementing
//! [DatabaseCatalogEntry], [DatabaseSchemaEntry], and [DatabaseTableEntry],
//! respectively. These can be concrete Rust structs, such as [SimpleCatalogEntry],
//! [SimpleSchemaEntry], and [SimpleTableEntry]. Or they can be zero-copy views
//! onto Arrow record batches as returned by the C API ADBC drivers (TODO).
//!
//! | Trait                        | Simple Rust-based    |
//! |------------------------------|----------------------|
//! | [DatabaseCatalogCollection]  | [SimpleSchemaEntry]  |
//! | [DatabaseCatalogEntry]       | [SimpleCatalogEntry] |
//! | [DatabaseSchemaEntry]        | [SimpleSchemaEntry]  |
//! | [DatabaseTableEntry]         | [SimpleTableEntry]   |
//!
//! There are owned and reference variations of columns, table constraints,
//! and foreign key usage. Each have a `borrow()` method to transform a owned
//! variant into its reference variant, and a `to_owned()` method to transform the
//! reference variant into the owned. These mimic the [std::borrow::Borrow] and
//! [std::borrow::ToOwned] traits, but do not actually implement them.
//!
//! | Owned             | Reference            |
//! |-------------------|----------------------|
//! | [ColumnSchema]    | [ColumnSchemaRef]    |
//! | [TableConstraint] | [TableConstraintRef] |
//! | [ForeignKeyUsage] | [ForeignKeyUsageRef] |

/// A collection of database catalogs, returned by [crate::AdbcConnection::get_objects].
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
    /// This is case sensitive.
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
    /// Use [crate::AdbcConnection::get_table_types] to get a list of supported types for
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
#[derive(Debug, Clone, Default, PartialEq)]
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
#[derive(Debug, Clone, PartialEq)]
pub struct TableConstraint {
    name: Option<String>,
    columns: Vec<String>,
    constraint_type: TableConstraintType,
}

#[derive(Debug, Clone, PartialEq)]
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
#[derive(Debug, Clone, Default, PartialEq)]
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

/// A simple collection of database objects, made up of Rust data structures.
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleCatalogCollection {
    catalogs: Vec<SimpleCatalogEntry>,
}

impl SimpleCatalogCollection {
    pub fn new(catalogs: Vec<SimpleCatalogEntry>) -> Self {
        Self { catalogs }
    }
}

impl DatabaseCatalogCollection for SimpleCatalogCollection {
    type CatalogEntryType<'a> = &'a SimpleCatalogEntry;
    fn catalogs<'a>(&'a self) -> Box<dyn Iterator<Item = Self::CatalogEntryType<'a>> + 'a> {
        Box::new(self.catalogs.iter())
    }
}

/// A single database catalog. See [DatabaseCatalogEntry].
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleCatalogEntry {
    name: Option<String>,
    db_schemas: Vec<SimpleSchemaEntry>,
}

impl SimpleCatalogEntry {
    pub fn new(name: Option<String>, db_schemas: Vec<SimpleSchemaEntry>) -> Self {
        Self { name, db_schemas }
    }
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
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleSchemaEntry {
    name: Option<String>,
    tables: Vec<SimpleTableEntry>,
}

impl SimpleSchemaEntry {
    pub fn new(name: Option<String>, tables: Vec<SimpleTableEntry>) -> Self {
        Self { name, tables }
    }
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
#[derive(Debug, Clone, PartialEq)]
pub struct SimpleTableEntry {
    name: String,
    table_type: String,
    columns: Vec<ColumnSchema>,
    constraints: Vec<TableConstraint>,
}

impl SimpleTableEntry {
    pub fn new(
        name: String,
        table_type: String,
        columns: Vec<ColumnSchema>,
        constraints: Vec<TableConstraint>,
    ) -> Self {
        Self {
            name,
            table_type,
            columns,
            constraints,
        }
    }
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
