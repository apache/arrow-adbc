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
using System.Data.Common;
using System.Linq;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using DuckDB.NET.Data;
using DuckDBNETConnection = DuckDB.NET.Data.DuckDBConnection;

namespace Apache.Arrow.Adbc.Drivers.DuckDB
{
    using GetObjectsDepth = Apache.Arrow.Adbc.AdbcConnection.GetObjectsDepth;
    /// <summary>
    /// Builds Arrow data for GetObjects metadata requests.
    /// </summary>
    internal class GetObjectsReader
    {
        private readonly DuckDBNETConnection _connection;
        private readonly GetObjectsDepth _depth;
        private readonly string? _catalogPattern;
        private readonly string? _dbSchemaPattern;
        private readonly string? _tableNamePattern;
        private readonly IReadOnlyList<string>? _tableTypes;
        private readonly string? _columnNamePattern;

        public GetObjectsReader(
            DuckDBNETConnection connection,
            GetObjectsDepth depth,
            string? catalogPattern,
            string? dbSchemaPattern,
            string? tableNamePattern,
            IReadOnlyList<string>? tableTypes,
            string? columnNamePattern)
        {
            _connection = connection;
            _depth = depth;
            _catalogPattern = catalogPattern;
            _dbSchemaPattern = dbSchemaPattern;
            _tableNamePattern = tableNamePattern;
            _tableTypes = tableTypes;
            _columnNamePattern = columnNamePattern;
        }

        public IArrowArrayStream Build()
        {
            var catalogs = GetCatalogs();
            
            // Build the result according to ADBC GetObjects specification
            var schema = StandardSchemas.GetObjectsSchema;
            var batches = new List<RecordBatch>();
            
            if (catalogs.Any())
            {
                var catalogNameBuilder = new StringArray.Builder();
                var dbSchemaListBuilder = new ListArray.Builder(StandardSchemas.DbSchemaSchema);
                
                foreach (var catalog in catalogs)
                {
                    catalogNameBuilder.Append(catalog);
                    
                    if (_depth >= GetObjectsDepth.DbSchemas)
                    {
                        var schemas = GetDbSchemas(catalog);
                        AppendDbSchemas(dbSchemaListBuilder, catalog, schemas);
                    }
                    else
                    {
                        dbSchemaListBuilder.AppendNull();
                    }
                }
                
                var batch = new RecordBatch(schema, new IArrowArray[]
                {
                    catalogNameBuilder.Build(),
                    dbSchemaListBuilder.Build()
                }, catalogs.Count);
                
                batches.Add(batch);
            }
            
            return new ListArrayStream(schema, batches);
        }

        private void AppendDbSchemas(ListArray.Builder dbSchemaListBuilder, string catalog, List<string> schemas)
        {
            if (!schemas.Any())
            {
                dbSchemaListBuilder.AppendNull();
                return;
            }
            
            var dbSchemaStructBuilder = new StructArray.Builder(StandardSchemas.DbSchemaSchema);
            var dbSchemaNameBuilder = dbSchemaStructBuilder.Field("db_schema_name", new StringArray.Builder());
            var dbSchemaTablesBuilder = dbSchemaStructBuilder.Field("db_schema_tables", 
                new ListArray.Builder(StandardSchemas.TableSchema));
            
            foreach (var schema in schemas)
            {
                dbSchemaNameBuilder.Append(schema);
                
                if (_depth >= GetObjectsDepth.Tables)
                {
                    var tables = GetTables(catalog, schema);
                    AppendTables(dbSchemaTablesBuilder as ListArray.Builder, catalog, schema, tables);
                }
                else
                {
                    dbSchemaTablesBuilder.AppendNull();
                }
            }
            
            dbSchemaListBuilder.Append();
        }

        private void AppendTables(ListArray.Builder? tableListBuilder, string catalog, string schema, List<(string name, string type)> tables)
        {
            if (tableListBuilder == null || !tables.Any())
            {
                tableListBuilder?.AppendNull();
                return;
            }
            
            var tableStructBuilder = new StructArray.Builder(StandardSchemas.TableSchema);
            var tableNameBuilder = tableStructBuilder.Field("table_name", new StringArray.Builder());
            var tableTypeBuilder = tableStructBuilder.Field("table_type", new StringArray.Builder());
            var tableColumnsBuilder = tableStructBuilder.Field("table_columns",
                new ListArray.Builder(StandardSchemas.ColumnSchema));
            var tableConstraintsBuilder = tableStructBuilder.Field("table_constraints",
                new ListArray.Builder(StandardSchemas.ConstraintSchema));
            
            foreach (var (tableName, tableType) in tables)
            {
                tableNameBuilder.Append(tableName);
                tableTypeBuilder.Append(tableType);
                
                if (_depth >= GetObjectsDepth.Columns)
                {
                    var columns = GetColumns(catalog, schema, tableName);
                    AppendColumns(tableColumnsBuilder as ListArray.Builder, columns);
                }
                else
                {
                    tableColumnsBuilder.AppendNull();
                }
                
                // DuckDB doesn't expose constraint information easily
                tableConstraintsBuilder.AppendNull();
            }
            
            tableListBuilder.Append();
        }

        private void AppendColumns(ListArray.Builder? columnListBuilder, List<ColumnInfo> columns)
        {
            if (columnListBuilder == null || !columns.Any())
            {
                columnListBuilder?.AppendNull();
                return;
            }
            
            var columnStructBuilder = new StructArray.Builder(StandardSchemas.ColumnSchema);
            var columnNameBuilder = columnStructBuilder.Field("column_name", new StringArray.Builder());
            var ordinalPositionBuilder = columnStructBuilder.Field("ordinal_position", new Int32Array.Builder());
            var remarksBuilder = columnStructBuilder.Field("remarks", new StringArray.Builder());
            var xdbcDataTypeBuilder = columnStructBuilder.Field("xdbc_data_type", new Int16Array.Builder());
            var xdbcTypeNameBuilder = columnStructBuilder.Field("xdbc_type_name", new StringArray.Builder());
            var xdbcColumnSizeBuilder = columnStructBuilder.Field("xdbc_column_size", new Int32Array.Builder());
            var xdbcDecimalDigitsBuilder = columnStructBuilder.Field("xdbc_decimal_digits", new Int16Array.Builder());
            var xdbcNumPrecRadixBuilder = columnStructBuilder.Field("xdbc_num_prec_radix", new Int16Array.Builder());
            var xdbcNullableBuilder = columnStructBuilder.Field("xdbc_nullable", new Int16Array.Builder());
            var xdbcColumnDefBuilder = columnStructBuilder.Field("xdbc_column_def", new StringArray.Builder());
            var xdbcSqlDataTypeBuilder = columnStructBuilder.Field("xdbc_sql_data_type", new Int16Array.Builder());
            var xdbcDatetimeSubBuilder = columnStructBuilder.Field("xdbc_datetime_sub", new Int16Array.Builder());
            var xdbcCharOctetLengthBuilder = columnStructBuilder.Field("xdbc_char_octet_length", new Int32Array.Builder());
            var xdbcIsNullableBuilder = columnStructBuilder.Field("xdbc_is_nullable", new StringArray.Builder());
            var xdbcScopeCatalogBuilder = columnStructBuilder.Field("xdbc_scope_catalog", new StringArray.Builder());
            var xdbcScopeSchemaBuilder = columnStructBuilder.Field("xdbc_scope_schema", new StringArray.Builder());
            var xdbcScopeTableBuilder = columnStructBuilder.Field("xdbc_scope_table", new StringArray.Builder());
            var xdbcIsAutoincrementBuilder = columnStructBuilder.Field("xdbc_is_autoincrement", new BooleanArray.Builder());
            var xdbcIsGeneratedcolumnBuilder = columnStructBuilder.Field("xdbc_is_generatedcolumn", new BooleanArray.Builder());
            
            foreach (var column in columns)
            {
                columnNameBuilder.Append(column.Name);
                ordinalPositionBuilder.Append(column.OrdinalPosition);
                remarksBuilder.AppendNull(); // No remarks in DuckDB
                xdbcDataTypeBuilder.AppendNull(); // Would need to map DuckDB types to XDBC types
                xdbcTypeNameBuilder.Append(column.DataType);
                xdbcColumnSizeBuilder.Append(column.CharacterMaximumLength ?? -1);
                xdbcDecimalDigitsBuilder.Append((short)(column.NumericScale ?? -1));
                xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.Append(column.IsNullable ? (short)1 : (short)0);
                xdbcColumnDefBuilder.Append(column.ColumnDefault);
                xdbcSqlDataTypeBuilder.AppendNull();
                xdbcDatetimeSubBuilder.AppendNull();
                xdbcCharOctetLengthBuilder.Append(column.CharacterMaximumLength ?? -1);
                xdbcIsNullableBuilder.Append(column.IsNullable ? "YES" : "NO");
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.Append(false); // DuckDB doesn't have autoincrement
                xdbcIsGeneratedcolumnBuilder.Append(false);
            }
            
            columnListBuilder.Append();
        }

        private List<string> GetCatalogs()
        {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT DISTINCT catalog_name FROM information_schema.schemata";
            
            if (!string.IsNullOrEmpty(_catalogPattern))
            {
                command.CommandText += " WHERE catalog_name LIKE ?";
                var param = command.CreateParameter();
                param.Value = _catalogPattern;
                command.Parameters.Add(param);
            }
            
            command.CommandText += " ORDER BY catalog_name";
            
            using var reader = command.ExecuteReader();
            var catalogs = new List<string>();
            
            while (reader.Read())
            {
                catalogs.Add(reader.GetString(0));
            }
            
            return catalogs;
        }

        private List<string> GetDbSchemas(string catalog)
        {
            using var command = _connection.CreateCommand();
            command.CommandText = "SELECT DISTINCT schema_name FROM information_schema.schemata WHERE catalog_name = ?";
            
            var param = command.CreateParameter();
            param.Value = catalog;
            command.Parameters.Add(param);
            
            if (!string.IsNullOrEmpty(_dbSchemaPattern))
            {
                command.CommandText += " AND schema_name LIKE ?";
                var schemaParam = command.CreateParameter();
                schemaParam.Value = _dbSchemaPattern;
                command.Parameters.Add(schemaParam);
            }
            
            command.CommandText += " ORDER BY schema_name";
            
            using var reader = command.ExecuteReader();
            var schemas = new List<string>();
            
            while (reader.Read())
            {
                schemas.Add(reader.GetString(0));
            }
            
            return schemas;
        }

        private List<(string name, string type)> GetTables(string catalog, string schema)
        {
            using var command = _connection.CreateCommand();
            command.CommandText = @"
                SELECT table_name, table_type 
                FROM information_schema.tables 
                WHERE table_catalog = ? AND table_schema = ?";
            
            var catalogParam = command.CreateParameter();
            catalogParam.Value = catalog;
            command.Parameters.Add(catalogParam);
            
            var schemaParam = command.CreateParameter();
            schemaParam.Value = schema;
            command.Parameters.Add(schemaParam);
            
            if (!string.IsNullOrEmpty(_tableNamePattern))
            {
                command.CommandText += " AND table_name LIKE ?";
                var tableParam = command.CreateParameter();
                tableParam.Value = _tableNamePattern;
                command.Parameters.Add(tableParam);
            }
            
            if (_tableTypes != null && _tableTypes.Any())
            {
                var placeholders = string.Join(", ", _tableTypes.Select(_ => "?"));
                command.CommandText += $" AND table_type IN ({placeholders})";
                
                foreach (var tableType in _tableTypes)
                {
                    var typeParam = command.CreateParameter();
                    typeParam.Value = tableType;
                    command.Parameters.Add(typeParam);
                }
            }
            
            command.CommandText += " ORDER BY table_name";
            
            using var reader = command.ExecuteReader();
            var tables = new List<(string, string)>();
            
            while (reader.Read())
            {
                tables.Add((reader.GetString(0), reader.GetString(1)));
            }
            
            return tables;
        }

        private List<ColumnInfo> GetColumns(string catalog, string schema, string tableName)
        {
            using var command = _connection.CreateCommand();
            command.CommandText = @"
                SELECT 
                    column_name,
                    ordinal_position,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length,
                    numeric_precision,
                    numeric_scale
                FROM information_schema.columns
                WHERE table_catalog = ? AND table_schema = ? AND table_name = ?";
            
            var catalogParam = command.CreateParameter();
            catalogParam.Value = catalog;
            command.Parameters.Add(catalogParam);
            
            var schemaParam = command.CreateParameter();
            schemaParam.Value = schema;
            command.Parameters.Add(schemaParam);
            
            var tableParam = command.CreateParameter();
            tableParam.Value = tableName;
            command.Parameters.Add(tableParam);
            
            if (!string.IsNullOrEmpty(_columnNamePattern))
            {
                command.CommandText += " AND column_name LIKE ?";
                var columnParam = command.CreateParameter();
                columnParam.Value = _columnNamePattern;
                command.Parameters.Add(columnParam);
            }
            
            command.CommandText += " ORDER BY ordinal_position";
            
            using var reader = command.ExecuteReader();
            var columns = new List<ColumnInfo>();
            
            while (reader.Read())
            {
                columns.Add(new ColumnInfo
                {
                    Name = reader.GetString(0),
                    OrdinalPosition = reader.GetInt32(1),
                    DataType = reader.GetString(2),
                    IsNullable = reader.GetString(3) == "YES",
                    ColumnDefault = reader.IsDBNull(4) ? null : reader.GetString(4),
                    CharacterMaximumLength = reader.IsDBNull(5) ? null : reader.GetInt32(5),
                    NumericPrecision = reader.IsDBNull(6) ? null : reader.GetInt32(6),
                    NumericScale = reader.IsDBNull(7) ? null : reader.GetInt32(7)
                });
            }
            
            return columns;
        }

        private class ColumnInfo
        {
            public string Name { get; set; } = string.Empty;
            public int OrdinalPosition { get; set; }
            public string DataType { get; set; } = string.Empty;
            public bool IsNullable { get; set; }
            public string? ColumnDefault { get; set; }
            public int? CharacterMaximumLength { get; set; }
            public int? NumericPrecision { get; set; }
            public int? NumericScale { get; set; }
        }
    }
}