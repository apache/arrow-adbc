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

using System.Collections.Generic;

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
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <returns></returns>
        public static List<AdbcCatalog> ParseCatalog(RecordBatch recordBatch, string databaseName, string schemaName)
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

        private static List<AdbcDbSchema> ParseDbSchema(StructArray dbSchemaArray, string schemaName)
        {
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

        private static List<AdbcTable> ParseTables(StructArray tablesArray)
        {
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
                    Columns = ParseColumns((StructArray)columnsArray.GetSlicedValues(i))
                });
            }

            return tables;
        }

        private static List<AdbcColumn> ParseColumns(StructArray columnsArray)
        {
            List<AdbcColumn> columns = new List<AdbcColumn>();

            StringArray column_name = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "column_name")]; // column_name | utf8 not null
            Int32Array ordinal_position = (Int32Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "ordinal_position")]; //	ordinal_position | int32
            StringArray remarks = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "remarks")]; //	remarks | utf8
            Int16Array xdbc_data_type = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_data_type")]; // xdbc_data_type | int16
            StringArray xdbc_type_name = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_type_name")]; // xdbc_type_name | utf8
            Int32Array xdbc_column_size = (Int32Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_column_size")]; // xdbc_column_size | int32
            Int16Array xdbc_decimal_digits = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_decimal_digits")]; //		xdbc_decimal_digits	| int16
            Int16Array xdbc_num_prec_radix = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_num_prec_radix")];//		xdbc_num_prec_radix	| int16
            Int16Array xdbc_nullable = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_nullable")];//		xdbc_nullable	| int16
            StringArray xdbc_column_def = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_column_def")]; //		xdbc_column_def	| utf8
            Int16Array xdbc_sql_data_type = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_sql_data_type")];//		xdbc_sql_data_type	| int16
            Int16Array xdbc_datetime_sub = (Int16Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_datetime_sub")]; //		xdbc_datetime_sub   | int16
            Int32Array xdbc_char_octet_length = (Int32Array)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_char_octet_length")];  //		xdbc_char_octet_length	| int32
            StringArray xdbc_is_nullable = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_is_nullable")]; //		xdbc_is_nullable | utf8
            StringArray xdbc_scope_catalog = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_scope_catalog")];//		xdbc_scope_catalog | utf8
            StringArray xdbc_scope_schema = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_scope_schema")]; //		xdbc_scope_schema | utf8
            StringArray xdbc_scope_table = (StringArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_scope_table")]; //		xdbc_scope_table | utf8
            BooleanArray xdbc_is_autoincrement = (BooleanArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_is_autoincrement")]; //		xdbc_is_autoincrement | bool
            BooleanArray xdbc_is_generatedcolumn = (BooleanArray)columnsArray.Fields[StandardSchemas.ColumnSchema.FindIndex(f => f.Name == "xdbc_is_generatedcolumn")]; //		xdbc_is_generatedcolumn | bool

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
    }
}
