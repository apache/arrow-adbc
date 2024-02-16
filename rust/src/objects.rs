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
//! onto Arrow record batches as returned by the C API ADBC drivers. The latter is
//! implemented in [ImportedCatalogCollection][crate::driver_manager::ImportedCatalogCollection].
//!
//! | Trait                        | Simple Rust-based         | Arrow RecordBatch view |
//! |------------------------------|---------------------------|------------------------|
//! | [DatabaseCatalogCollection]  | [SimpleCatalogCollection] | [ImportedCatalogCollection][crate::driver_manager::ImportedCatalogCollection] |
//! | [DatabaseCatalogEntry]       | [SimpleCatalogEntry]      | [ImportedCatalogEntry][crate::driver_manager::ImportedCatalogEntry] |
//! | [DatabaseSchemaEntry]        | [SimpleSchemaEntry]       | [ImportedSchemaEntry][crate::driver_manager::ImportedSchemaEntry] |
//! | [DatabaseTableEntry]         | [SimpleTableEntry]        | [ImportedTableEntry][crate::driver_manager::ImportedTableEntry] |
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
use std::sync::Arc;

use arrow::buffer::OffsetBuffer;
use arrow_array::builder::{
    ArrayBuilder, BooleanBufferBuilder, BooleanBuilder, Int16Builder, Int32BufferBuilder,
    Int32Builder, ListBuilder, StringBuilder,
};
use arrow_array::{ArrayRef, ListArray, RecordBatch, StructArray};
use arrow_schema::{DataType, Field};

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

    pub fn schema() -> Vec<Arc<Field>> {
        vec![
            Arc::new(Field::new("fk_catalog", DataType::Utf8, true)),
            Arc::new(Field::new("fk_db_schema", DataType::Utf8, true)),
            Arc::new(Field::new("fk_table", DataType::Utf8, false)),
            Arc::new(Field::new("fk_column_name", DataType::Utf8, false)),
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

    pub fn schema() -> Vec<Arc<Field>> {
        let usage_schema = DataType::Struct(UsageArrayBuilder::schema().into());
        vec![
            Arc::new(Field::new("constraint_name", DataType::Utf8, true)),
            Arc::new(Field::new("constraint_type", DataType::Utf8, false)),
            Arc::new(Field::new(
                "constraint_column_names",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                false,
            )),
            Arc::new(Field::new(
                "constraint_column_usage",
                DataType::List(Arc::new(Field::new("item", usage_schema, true))),
                true,
            )),
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
        let constraint_column_usage_type = DataType::Struct(UsageArrayBuilder::schema().into());
        let constraint_column_usage = ListArray::new(
            Arc::new(Field::new("item", constraint_column_usage_type, true)),
            OffsetBuffer::new(self.usage_offsets.finish().into()),
            Arc::new(self.usage.finish()),
            Some(self.usage_validity.finish().into()),
        );

        let arrays: &[ArrayRef; 4] = &[
            Arc::new(self.constraint_name.finish()),
            Arc::new(self.constraint_type.finish()),
            Arc::new(self.constraint_cols.finish()),
            Arc::new(constraint_column_usage),
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
            xdbc_scope_catalog: StringBuilder::with_capacity(num_columns, xdbc_scope_catalog_len),
            xdbc_scope_schema: StringBuilder::with_capacity(num_columns, xdbc_scope_schema_len),
            xdbc_scope_table: StringBuilder::with_capacity(num_columns, xdbc_scope_table_len),
            xdbc_is_autoincrement: BooleanBuilder::with_capacity(num_columns),
            xdbc_is_generatedcolumn: BooleanBuilder::with_capacity(num_columns),
        }
    }

    pub fn schema() -> Vec<Arc<Field>> {
        vec![
            Arc::new(Field::new("column_name", DataType::Utf8, false)),
            Arc::new(Field::new("ordinal_position", DataType::Int32, true)),
            Arc::new(Field::new("remarks", DataType::Utf8, true)),
            Arc::new(Field::new("xdbc_data_type", DataType::Int16, true)),
            Arc::new(Field::new("xdbc_type_name", DataType::Utf8, true)),
            Arc::new(Field::new("xdbc_column_size", DataType::Int32, true)),
            Arc::new(Field::new("xdbc_decimal_digits", DataType::Int16, true)),
            Arc::new(Field::new("xdbc_num_prec_radix", DataType::Int16, true)),
            Arc::new(Field::new("xdbc_nullable", DataType::Int16, true)),
            Arc::new(Field::new("xdbc_column_def", DataType::Utf8, true)),
            Arc::new(Field::new("xdbc_sql_data_type", DataType::Int16, true)),
            Arc::new(Field::new("xdbc_datetime_sub", DataType::Int16, true)),
            Arc::new(Field::new("xdbc_char_octet_length", DataType::Int32, true)),
            Arc::new(Field::new("xdbc_is_nullable", DataType::Utf8, true)),
            Arc::new(Field::new("xdbc_scope_catalog", DataType::Utf8, true)),
            Arc::new(Field::new("xdbc_scope_schema", DataType::Utf8, true)),
            Arc::new(Field::new("xdbc_scope_table", DataType::Utf8, true)),
            Arc::new(Field::new("xdbc_is_autoincrement", DataType::Boolean, true)),
            Arc::new(Field::new(
                "xdbc_is_generatedcolumn",
                DataType::Boolean,
                true,
            )),
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
    pub fn with_capacity(catalogs: &impl DatabaseCatalogCollection) -> Self {
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

    pub fn schema() -> Vec<Arc<Field>> {
        let constraint_schema = DataType::Struct(ConstraintArrayBuilder::schema().into());
        let column_schema = DataType::Struct(ColumnArrayBuilder::schema().into());

        vec![
            Arc::new(Field::new("table_name", DataType::Utf8, false)),
            Arc::new(Field::new("table_type", DataType::Utf8, false)),
            Arc::new(Field::new(
                "table_columns",
                DataType::List(Arc::new(Field::new("item", column_schema, true))),
                true,
            )),
            Arc::new(Field::new(
                "table_constraints",
                DataType::List(Arc::new(Field::new("item", constraint_schema, true))),
                true,
            )),
        ]
    }

    pub fn append_tables<'a>(&mut self, tables: impl Iterator<Item = impl DatabaseTableEntry<'a>>) {
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
        let column_schema_type = DataType::Struct(ColumnArrayBuilder::schema().into());
        let table_columns = ListArray::new(
            Arc::new(Field::new("item", column_schema_type, true)),
            OffsetBuffer::new(self.columns_offsets.finish().into()),
            Arc::new(self.table_columns.finish()),
            Some(self.columns_validity.finish().into()),
        );

        let constraint_schema_type = DataType::Struct(ConstraintArrayBuilder::schema().into());
        let table_constraints = ListArray::new(
            Arc::new(Field::new("item", constraint_schema_type, true)),
            OffsetBuffer::new(self.constraints_offsets.finish().into()),
            Arc::new(self.table_constraints.finish()),
            Some(self.constraints_validity.finish().into()),
        );

        let arrays: &[ArrayRef; 4] = &[
            Arc::new(self.table_name.finish()),
            Arc::new(self.table_type.finish()),
            Arc::new(table_columns),
            Arc::new(table_constraints),
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

    pub fn schema() -> Vec<Arc<Field>> {
        let table_schema = DataType::Struct(TableArrayBuilder::schema().into());

        vec![
            Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
            Arc::new(Field::new(
                "db_schema_tables",
                DataType::List(Arc::new(Field::new("item", table_schema, true))),
                true,
            )),
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
        let table_schema_type = DataType::Struct(TableArrayBuilder::schema().into());
        let db_schema_tables = ListArray::new(
            Arc::new(Field::new("item", table_schema_type, true)),
            OffsetBuffer::new(self.tables_offsets.finish().into()),
            Arc::new(self.tables.finish()),
            Some(self.tables_validity.finish().into()),
        );

        let arrays: &[ArrayRef; 2] = &[
            Arc::new(self.db_schema_name.finish()),
            Arc::new(db_schema_tables),
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

    pub fn schema() -> Vec<Arc<Field>> {
        let db_schema_schema = DataType::Struct(DbSchemaArrayBuilder::schema().into());

        vec![
            Arc::new(Field::new("catalog_name", DataType::Utf8, true)),
            Arc::new(Field::new(
                "catalog_db_schemas",
                DataType::List(Arc::new(Field::new("item", db_schema_schema, true))),
                true,
            )),
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
        let db_schema_schema_type = DataType::Struct(DbSchemaArrayBuilder::schema().into());
        let catalog_db_schemas = ListArray::new(
            Arc::new(Field::new("item", db_schema_schema_type, true)),
            OffsetBuffer::new(self.schemas_offsets.finish().into()),
            Arc::new(self.schemas.finish()),
            Some(self.schemas_validity.finish().into()),
        );

        let arrays: &[ArrayRef; 2] = &[
            Arc::new(self.catalog_name.finish()),
            Arc::new(catalog_db_schemas),
        ];

        StructArray::from(
            Self::schema()
                .into_iter()
                .zip(arrays.iter().cloned())
                .collect::<Vec<_>>(),
        )
    }
}

/// A collection of database catalogs, returned by [crate::AdbcConnection::get_objects].
pub trait DatabaseCatalogCollection {
    type CatalogEntryType<'a>: DatabaseCatalogEntry<'a>
    where
        Self: 'a;
    type CatalogIterator<'a>: Iterator<Item = Self::CatalogEntryType<'a>> + 'a
    where
        Self: 'a;

    /// List all catalogs in the result set.
    fn catalogs(&self) -> Self::CatalogIterator<'_>;

    /// Get a particular catalog by name.
    ///
    /// Databases that have no notion of catalogs will have one with None for a name.
    /// This is case sensitive.
    fn catalog(&self, name: Option<&str>) -> Option<Self::CatalogEntryType<'_>> {
        self.catalogs().find(|catalog| catalog.name() == name)
    }

    /// Get as a record batch, for exporting in the C ADBC API.
    fn to_record_batch(&self) -> RecordBatch
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
    type SchemaIterator: Iterator<Item = Self::SchemaEntryType> + 'a;

    /// Get the name of the catalog.
    fn name(&self) -> Option<&'a str>;

    /// List all schemas in this catalog that are in the result set.
    fn schemas(&self) -> Self::SchemaIterator;

    /// Get a particular schema by name.
    ///
    /// Databases that have no notion of schemas will have one with None for a name.
    /// This is case sensitive.
    fn schema(&self, name: Option<&str>) -> Option<Self::SchemaEntryType> {
        self.schemas().find(|schema| schema.name() == name)
    }
}

/// An entry in [DatabaseCatalogCollection] representing a single schema.
pub trait DatabaseSchemaEntry<'a> {
    type TableEntryType: DatabaseTableEntry<'a>;
    type TableIterator: Iterator<Item = Self::TableEntryType> + 'a;

    /// Get the name of the schema.
    fn name(&self) -> Option<&'a str>;

    /// List all the tables in this schema that are in the result set.
    fn tables(&self) -> Self::TableIterator;

    /// Get a particular table by name.
    ///
    /// This is case sensitive.
    fn table(&self, name: &str) -> Option<Self::TableEntryType> {
        self.tables().find(|table| table.name() == name)
    }
}

/// An entry in the [DatabaseCatalogCollection] representing a single table.
pub trait DatabaseTableEntry<'a> {
    type ColumnIterator: Iterator<Item = ColumnSchemaRef<'a>> + 'a;
    type ConstraintIterator: Iterator<Item = TableConstraintRef<'a>> + 'a;

    /// The name of the table.
    fn name(&self) -> &'a str;

    /// The table type.
    ///
    /// Use [crate::AdbcConnection::get_table_types] to get a list of supported types for
    /// the database.
    fn table_type(&self) -> &'a str;

    /// List all the columns in the table.
    fn columns(&self) -> Self::ColumnIterator;

    /// Get a column for a particular ordinal position.
    ///
    /// Will return None if the column is not found.
    fn column(&self, i: i32) -> Option<ColumnSchemaRef<'a>> {
        self.columns().find(|col| col.ordinal_position == i)
    }

    /// Get a column by name.
    ///
    /// This is case sensitive. Will return None if the column is not found.
    fn column_by_name(&self, name: &str) -> Option<ColumnSchemaRef<'a>> {
        self.columns().find(|col| col.name == name)
    }

    /// List all the constraints on the table.
    fn constraints(&self) -> Self::ConstraintIterator;
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

/// The type of table constraint. Used in [TableConstraintRef].
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
            Self::ForeignKey { usage: _ } => "FOREIGN KEY",
            Self::PrimaryKey => "PRIMARY KEY",
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
    type CatalogIterator<'a> = std::slice::Iter<'a, SimpleCatalogEntry>;
    fn catalogs(&self) -> Self::CatalogIterator<'_> {
        self.catalogs.iter()
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
    type SchemaIterator = std::slice::Iter<'a, SimpleSchemaEntry>;

    fn name(&self) -> Option<&'a str> {
        self.name.as_deref()
    }

    fn schemas(&self) -> Self::SchemaIterator {
        self.db_schemas.iter()
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
    type TableIterator = std::slice::Iter<'a, SimpleTableEntry>;

    fn name(&self) -> Option<&'a str> {
        self.name.as_deref()
    }

    fn tables(&self) -> Self::TableIterator {
        self.tables.iter()
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
    type ColumnIterator = std::iter::Map<
        std::slice::Iter<'a, ColumnSchema>,
        fn(&ColumnSchema) -> ColumnSchemaRef<'_>,
    >;
    type ConstraintIterator = std::iter::Map<
        std::slice::Iter<'a, TableConstraint>,
        fn(&TableConstraint) -> TableConstraintRef<'_>,
    >;

    fn name(&self) -> &'a str {
        &self.name
    }

    fn table_type(&self) -> &'a str {
        &self.table_type
    }

    fn columns(&self) -> Self::ColumnIterator {
        self.columns.iter().map(|col| col.borrow())
    }

    fn constraints(&self) -> Self::ConstraintIterator {
        self.constraints
            .iter()
            .map(|constraint| constraint.borrow())
    }
}
