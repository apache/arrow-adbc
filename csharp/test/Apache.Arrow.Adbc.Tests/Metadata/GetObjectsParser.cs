/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Linq;

namespace Apache.Arrow.Adbc.Tests.Metadata
{
    /// <summary>
    /// Parses a <see cref="RecordBatch"/> from a GetObjects call
    /// </summary>
    public class GetObjectsParser
    {
        /// <summary>
        /// Parses a <see cref="RecordBatch"/> from a GetObjects call for the <see cref="AdbcCatalog"/>.
        /// </summary>
        /// <param name="recordBatch"></param>
        /// <param name="schemaName"></param>
        /// <returns></returns>
        public static List<AdbcCatalog> ParseCatalog(RecordBatch recordBatch, string? schemaName)
        {
            StringArray catalogNameArray = (StringArray)recordBatch.Column("catalog_name");
            ListArray dbSchemaArray = (ListArray)recordBatch.Column("catalog_db_schemas");

            List<AdbcCatalog> catalogs = new List<AdbcCatalog>();

            for (int i = 0; i < catalogNameArray.Length; i++)
            {
                catalogs.Add(new AdbcCatalog()
                {
                    Name = catalogNameArray.GetString(i),
                    DbSchemas = ParseDbSchema((StructArray)dbSchemaArray.GetSlicedValues(i), schemaName)
                });
            }

            return catalogs;
        }

        private static List<AdbcDbSchema>? ParseDbSchema(StructArray dbSchemaArray, string? schemaName)
        {
            if (dbSchemaArray == null) return null;

            StringArray schemaNameArray = (StringArray)dbSchemaArray.Fields[0]; // db_schema_name
            ListArray tablesArray = (ListArray)dbSchemaArray.Fields[1]; // db_schema_tables

            List<AdbcDbSchema> schemas = new List<AdbcDbSchema>();

            for (int i = 0; i < dbSchemaArray.Length; i++)
            {
                schemas.Add(new AdbcDbSchema()
                {
                    Name = schemaNameArray.GetString(i),
                    Tables = ParseTables((StructArray)tablesArray.GetSlicedValues(i))
                });
            }

            return schemas;
        }

        private static List<AdbcTable>? ParseTables(StructArray? tablesArray)
        {
            if (tablesArray == null) return null;

            StringArray tableNameArray = (StringArray)tablesArray.Fields[0]; // table_name
            StringArray tableTypeArray = (StringArray)tablesArray.Fields[1]; // table_type
            ListArray columnsArray = (ListArray)tablesArray.Fields[2]; // table_columns
            ListArray tableConstraintsArray = (ListArray)tablesArray.Fields[3]; // table_constraints

            List<AdbcTable> tables = new List<AdbcTable>();

            for (int i = 0; i < tablesArray.Length; i++)
            {
                tables.Add(new AdbcTable()
                {
                    Name = tableNameArray.GetString(i),
                    Type = tableTypeArray.GetString(i),
                    Columns = ParseColumns((StructArray)columnsArray.GetSlicedValues(i)),
                    Constraints = ParseConstraints((StructArray)tableConstraintsArray.GetSlicedValues(i))
                });
            }

            return tables;
        }

        private static List<AdbcColumn>? ParseColumns(StructArray columnsArray)
        {
            if (columnsArray == null) return null;

            List<AdbcColumn> columns = new List<AdbcColumn>();

            StringArray column_name = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("column_name")]; // column_name | utf8 not null
            Int32Array ordinal_position = (Int32Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("ordinal_position")]; //	ordinal_position | int32
            StringArray remarks = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("remarks")]; //	remarks | utf8
            Int16Array xdbc_data_type = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_data_type")]; // xdbc_data_type | int16
            StringArray xdbc_type_name = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_type_name")]; // xdbc_type_name | utf8
            Int32Array xdbc_column_size = (Int32Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_column_size")]; // xdbc_column_size | int32
            Int16Array xdbc_decimal_digits = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_decimal_digits")]; //		xdbc_decimal_digits	| int16
            Int16Array xdbc_num_prec_radix = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_num_prec_radix")];//		xdbc_num_prec_radix	| int16
            Int16Array xdbc_nullable = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_nullable")];//		xdbc_nullable	| int16
            StringArray xdbc_column_def = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_column_def")]; //		xdbc_column_def	| utf8
            Int16Array xdbc_sql_data_type = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_sql_data_type")];//		xdbc_sql_data_type	| int16
            Int16Array xdbc_datetime_sub = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_datetime_sub")]; //		xdbc_datetime_sub   | int16
            Int32Array xdbc_char_octet_length = (Int32Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_char_octet_length")];  //		xdbc_char_octet_length	| int32
            StringArray xdbc_is_nullable = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_is_nullable")]; //		xdbc_is_nullable | utf8
            StringArray xdbc_scope_catalog = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_scope_catalog")];//		xdbc_scope_catalog | utf8
            StringArray xdbc_scope_schema = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_scope_schema")]; //		xdbc_scope_schema | utf8
            StringArray xdbc_scope_table = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_scope_table")]; //		xdbc_scope_table | utf8
            BooleanArray xdbc_is_autoincrement = (BooleanArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_is_autoincrement")]; //		xdbc_is_autoincrement | bool
            BooleanArray xdbc_is_generatedcolumn = (BooleanArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndexOrThrow("xdbc_is_generatedcolumn")]; //		xdbc_is_generatedcolumn | bool

            for (int i = 0; i < columnsArray.Length; i++)
            {
                AdbcColumn c = new AdbcColumn();
                c.Name = column_name.GetString(i);
                c.OrdinalPosition = ordinal_position.GetValue(i);
                c.Remarks = remarks.GetString(i);
                c.XdbcDataType = xdbc_data_type.GetValue(i);
                c.XdbcTypeName = xdbc_type_name.GetString(i);
                c.XdbcColumnSize = xdbc_column_size.GetValue(i);
                c.XdbcDecimalDigits = xdbc_decimal_digits.GetValue(i);
                c.XdbcNumPrecRadix = xdbc_num_prec_radix.GetValue(i);
                c.XdbcNullable = xdbc_nullable.GetValue(i);
                c.XdbcColumnDef = xdbc_column_def.GetString(i);
                c.XdbcSqlDataType = xdbc_sql_data_type.GetValue(i);
                c.XdbcDatetimeSub = xdbc_datetime_sub.GetValue(i);
                c.XdbcCharOctetLength = xdbc_char_octet_length.GetValue(i);
                c.XdbcIsNullable = xdbc_is_nullable.GetString(i);
                c.XdbcScopeCatalog = xdbc_scope_catalog.GetString(i);
                c.XdbcScopeSchema = xdbc_scope_schema.GetString(i);
                c.XdbcScopeTable = xdbc_scope_table.GetString(i);
                c.XdbcIsAutoIncrement = xdbc_is_autoincrement.GetValue(i);
                c.XdbcIsGeneratedColumn = xdbc_is_generatedcolumn.GetValue(i);

                columns.Add(c);
            }

            return columns;
        }

        private static List<AdbcConstraint>? ParseConstraints(StructArray constraintsArray)
        {
            if (constraintsArray == null) return null;

            // constraint details may not be loaded correctly if the depth wasn't Columns
            int fieldCount = constraintsArray?.Fields?.Count ?? 0;
            if (fieldCount == 0)
                return null;

            List<AdbcConstraint> constraints = new List<AdbcConstraint>();

            StringArray name = (StringArray)constraintsArray!.Fields[StandardSchemas.ConstraintSchema.FindIndexOrThrow("constraint_name")]; // constraint_name | utf8
            StringArray type = (StringArray)constraintsArray.Fields[StandardSchemas.ConstraintSchema.FindIndexOrThrow("constraint_type")]; //	constraint_type | utf8 not null
            ListArray columnNames = (ListArray)constraintsArray.Fields[StandardSchemas.ConstraintSchema.FindIndexOrThrow("constraint_column_names")]; //	constraint_column_names | list<utf8> not null
            ListArray columnUsages = (ListArray)constraintsArray.Fields[StandardSchemas.ConstraintSchema.FindIndexOrThrow("constraint_column_usage")]; //	constraint_column_usage | list<USAGE_SCHEMA>

            for (int i = 0; i < constraintsArray.Length; i++)
            {
                AdbcConstraint c = new AdbcConstraint();
                c.Name = name.GetString(i);
                c.Type = type.GetString(i);

                StringArray? colNames = columnNames.GetSlicedValues(i) as StringArray;
                StructArray? usages = columnUsages.GetSlicedValues(i) as StructArray;

                if (colNames != null)
                {
                    for (int j = 0; j < colNames.Length; j++)
                    {
                        c.ColumnNames.Add(colNames.GetString(j));
                    }
                }

                if (usages != null)
                {
                    StringArray fkCatalog = (StringArray)usages.Fields[StandardSchemas.UsageSchema.FindIndexOrThrow("fk_catalog")]; // fk_catalog	| utf8
                    StringArray fkDbSchema = (StringArray)usages.Fields[StandardSchemas.UsageSchema.FindIndexOrThrow("fk_db_schema")]; //fk_db_schema | utf8
                    StringArray fkTable = (StringArray)usages.Fields[StandardSchemas.UsageSchema.FindIndexOrThrow("fk_table")]; //	fk_table | utf8 not null
                    StringArray fkColumnName = (StringArray)usages.Fields[StandardSchemas.UsageSchema.FindIndexOrThrow("fk_column_name")]; // fk_column_name | utf8 not null

                    for (int j = 0; j < usages.Length; j++)
                    {

                        AdbcUsageSchema adbcUsageSchema = new AdbcUsageSchema();
                        adbcUsageSchema.FkCatalog = fkCatalog.GetString(j);
                        adbcUsageSchema.FkDbSchema = fkDbSchema.GetString(j);
                        adbcUsageSchema.FkTable = fkTable.GetString(j);
                        adbcUsageSchema.FkColumnName = fkColumnName.GetString(j);
                        c.ColumnUsage?.Add(adbcUsageSchema);
                    }
                }

                constraints.Add(c);
            }

            return constraints;
        }
    }

    /// <summary>
    /// Extension methods for List<Field> type
    /// </summary>
    ///
    public static class FieldExtensions
    {
        /// <summary>
        /// Finds the index of the first field with the provided name in the list or throws an exception
        /// </summary>
        /// <param name="fields">The list of fields</param>
        /// <param name="name">The field name to look for</param>
        /// <returns>The index of the first field with the provided name</returns>
        /// <exception cref="ArgumentNullException">Thrown if fields argument is null</exception>
        /// <exception cref="InvalidOperationException">Thrown if no matching field is found with the provided name</exception>
        public static int FindIndexOrThrow(this IReadOnlyList<Field> fields, string name)
        {
            if (fields == null)
            {
                throw new ArgumentNullException(nameof(fields));
            }
            for (int i = 0; i < fields.Count; i++)
            {
                if (fields[i].Name == name)
                {
                    return i;
                }
            }

            throw new InvalidOperationException($"No matching field found with name: {name}");
        }
    }
}
