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
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Google.Apis.Auth.OAuth2;
using Google.Cloud.BigQuery.V2;

namespace Apache.Arrow.Adbc.Drivers.BigQuery
{
    public class BigQueryConnection : AdbcConnection
    {
        readonly IReadOnlyDictionary<string, string> properties;
        BigQueryClient? client;
        GoogleCredential? credential;

        const string infoDriverName = "ADBC BigQuery Driver";
        const string infoDriverVersion = "1.0.0";
        const string infoVendorName = "BigQuery";
        const string infoDriverArrowVersion = "1.0.0";

        readonly IReadOnlyList<AdbcInfoCode> infoSupportedCodes = new List<AdbcInfoCode> {
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName
        };

        public BigQueryConnection(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
        }

        public void Open()
        {
            string projectId = string.Empty;
            string accessToken = string.Empty;

            if(!this.properties.TryGetValue(BigQueryParameters.ProjectId, out projectId))
                throw new ArgumentException($"The {BigQueryParameters.ProjectId} parameter is not present");

            if (!this.properties.TryGetValue(BigQueryParameters.AccessToken, out accessToken))
                throw new ArgumentException($"The {BigQueryParameters.AccessToken} parameter is not present");

            this.credential = GoogleCredential.FromAccessToken(accessToken);
            this.client = BigQueryClient.Create(projectId, this.credential);
        }

        public override IArrowArrayStream GetInfo(List<AdbcInfoCode> codes)
        {
            const int strValTypeID = 0;

            UnionType infoUnionType = new UnionType(
                            new List<Field>()
                            {
                                new Field("string_value", StringType.Default, true),
                                new Field("bool_value", BooleanType.Default, true),
                                new Field("int64_value", Int64Type.Default, true),
                                new Field("int32_bitmask", Int32Type.Default, true),
                                new Field(
                                    "string_list",
                                    new ListType(
                                        new Field("item", StringType.Default, true)
                                    ),
                                    false
                                ),
                                new Field(
                                    "int32_to_int32_list_map",
                                    new ListType(
                                        new Field("entries", new StructType(
                                            new List<Field>()
                                            {
                                                new Field("key", Int32Type.Default, false),
                                                new Field("value", Int32Type.Default, true),
                                            }
                                            ), false)
                                    ),
                                    true
                                )
                            },
                            new int[] { 0, 1, 2, 3, 4, 5 }.ToArray(),
                            UnionMode.Dense);

            if (codes.Count == 0)
            {
                codes = new List<AdbcInfoCode>(infoSupportedCodes);
            }

            var infoNameBuilder = new UInt32Array.Builder();
            var typeBuilder = new ArrowBuffer.Builder<byte>();
            var offsetBuilder = new ArrowBuffer.Builder<int>();
            var stringInfoBuilder = new StringArray.Builder();
            int nullCount = 0;
            int arrayLength = codes.Count;

            foreach (AdbcInfoCode code in codes)
            {
                switch (code)
                {
                    case AdbcInfoCode.DriverName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoDriverName);
                        break;
                    case AdbcInfoCode.DriverVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoDriverVersion);
                        break;
                    case AdbcInfoCode.DriverArrowVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoDriverArrowVersion);
                        break;
                    case AdbcInfoCode.VendorName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.Append(infoVendorName);
                        break;
                    default:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(stringInfoBuilder.Length);
                        stringInfoBuilder.AppendNull();
                        nullCount++;
                        break;
                }
            }

            var entryType = new StructType(
                new List<Field>(){
                    new Field("key", Int32Type.Default, false),
                    new Field("value", Int32Type.Default, true)});

            var entriesDataArray = new StructArray(entryType, 0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            var childrenArrays = new List<IArrowArray>()
            {
                stringInfoBuilder.Build(),
                new BooleanArray.Builder().Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                CreateNestedListArray(new List<IArrowArray?>(){ entriesDataArray }, entryType)
            };

            var infoValue = new DenseUnionArray(infoUnionType, arrayLength, childrenArrays, typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            var dataArrays = new List<IArrowArray>
            {
                infoNameBuilder.Build(),
                infoValue
            };

            return new BigQueryInfoArrowStream(StandardSchemas.GetInfoSchema, dataArrays, 4);
        }

        public override IArrowArrayStream GetObjects(
            GetObjectsDepth depth,
            string catalogPattern,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {
            var dataArrays = GetCatalogs(depth, catalogPattern, dbSchemaPattern,
                tableNamePattern, tableTypes, columnNamePattern);

            return new BigQueryInfoArrowStream(StandardSchemas.GetObjectsSchema, dataArrays, 1);
        }

        private List<IArrowArray> GetCatalogs(
            GetObjectsDepth depth,
            string catalogPattern,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {
            var catalogNameBuilder = new StringArray.Builder();
            var catalogDbSchemasValues = new List<IArrowArray?>();
            string catalogRegexp = PatternToRegexp(catalogPattern);
            var catalogs = this.client.ListProjects();

            foreach (var catalog in catalogs)
            {
                if (Regex.IsMatch(catalog.ProjectId, catalogRegexp, RegexOptions.IgnoreCase))
                {
                    catalogNameBuilder.Append(catalog.ProjectId);

                    if (depth == GetObjectsDepth.Catalogs)
                    {
                        catalogDbSchemasValues.Add(null);
                    }
                    else
                    {
                        catalogDbSchemasValues.Add(GetDbSchemas(
                            depth, catalog.ProjectId, dbSchemaPattern,
                            tableNamePattern, tableTypes, columnNamePattern));
                    }
                }
            }

            List<IArrowArray> dataArrays = new List<IArrowArray>
            {
                catalogNameBuilder.Build(),
                CreateNestedListArray(catalogDbSchemasValues, new StructType(StandardSchemas.DbSchemaSchema)),
            };

            return dataArrays;
        }

        private StructArray GetDbSchemas(
            GetObjectsDepth depth,
            string catalog,
            string dbSchemaPattern,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {
            StringArray.Builder dbSchemaNameBuilder = new StringArray.Builder();
            List<IArrowArray?> dbSchemaTablesValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;
            string dbSchemaRegexp = PatternToRegexp(dbSchemaPattern);
            var schemas = this.client.ListDatasets(catalog);
            foreach (var schema in schemas)
            {
                if (Regex.IsMatch(schema.Reference.DatasetId, dbSchemaRegexp, RegexOptions.IgnoreCase))
                {
                    dbSchemaNameBuilder.Append(schema.Reference.DatasetId);
                    length++;
                    nullBitmapBuffer.Append(true);

                    if (depth == GetObjectsDepth.DbSchemas)
                    {
                        dbSchemaTablesValues.Add(null);
                    }
                    else
                    {
                        dbSchemaTablesValues.Add(GetTableSchemas(
                            depth, catalog, schema.Reference.DatasetId,
                            tableNamePattern, tableTypes, columnNamePattern));
                    }
                }
            }

            var dataArrays = new List<IArrowArray>
            {
                dbSchemaNameBuilder.Build(),
                CreateNestedListArray(dbSchemaTablesValues, new StructType(StandardSchemas.TableSchema)),
            };

            return new StructArray(
                new StructType(StandardSchemas.DbSchemaSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StructArray GetTableSchemas(
            GetObjectsDepth depth,
            string catalog,
            string dbSchema,
            string tableNamePattern,
            List<string> tableTypes,
            string columnNamePattern)
        {
            var tableNameBuilder = new StringArray.Builder();
            var tableTypeBuilder = new StringArray.Builder();
            var tableColumnsValues = new List<IArrowArray?>();
            var tableConstraintsValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = String.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.TABLES",
                catalog, dbSchema);
            if (tableNamePattern != null)
            {
                query = String.Concat(query, String.Format(" WHERE table_name LIKE '{0}'", tableNamePattern));
                if (tableTypes.Count > 0)
                {
                    query = String.Concat(query, String.Format(" AND table_type IN ('{0}')", String.Join("', '", tableTypes).ToUpper()));
                }
            }
            else
            {
                if (tableTypes.Count > 0)
                {
                    query = String.Concat(query, String.Format(" WHERE table_type IN ('{0}')", String.Join("', '", tableTypes).ToUpper()));
                }
            }
            var result = this.client.ExecuteQuery(query, parameters: null);

            foreach (var row in result)
            {
                tableNameBuilder.Append(row["table_name"].ToString());
                tableTypeBuilder.Append(row["table_type"].ToString());
                nullBitmapBuffer.Append(true);
                length++;

                //tableConstraintsValues.Add(GetConstraintSchema(
                //    depth, catalog, dbSchema, row["table_name"].ToString(), columnNamePattern));

                // TODO: add constraints
                if (depth == GetObjectsDepth.Tables)
                {
                    tableColumnsValues.Add(null);
                }
                else
                {
                    tableColumnsValues.Add(GetColumnSchema(catalog, dbSchema, row["table_name"].ToString(), columnNamePattern));
                }
            }

            var dataArrays = new List<IArrowArray>
            {
                tableNameBuilder.Build(),
                tableTypeBuilder.Build(),
                CreateNestedListArray(tableColumnsValues, new StructType(StandardSchemas.ColumnSchema)),
                //CreateNestedListArray(tableConstraintsValues, new StructType(StandardSchemas.ConstraintSchema))
            };

            return new StructArray(
                new StructType(StandardSchemas.TableSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StructArray GetColumnSchema(
            string catalog,
            string dbSchema,
            string table,
            string columnNamePattern)
        {
            var columnNameBuilder = new StringArray.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var xdbcDataTypeBuilder = new Int16Array.Builder();
            var xdbcTypeNameBuilder = new StringArray.Builder();
            var xdbcColumnSizeBuilder = new Int32Array.Builder();
            var xdbcDecimalDigitsBuilder = new Int16Array.Builder();
            var xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            var xdbcNullableBuilder = new Int16Array.Builder();
            var xdbcColumnDefBuilder = new StringArray.Builder();
            var xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            var xdbcDatetimeSubBuilder = new Int16Array.Builder();
            var xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            var xdbcIsNullableBuilder = new StringArray.Builder();
            var xdbcScopeCatalogBuilder = new StringArray.Builder();
            var xdbcScopeSchemaBuilder = new StringArray.Builder();
            var xdbcScopeTableBuilder = new StringArray.Builder();
            var xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
            var xdbcIsGeneratedcolumnBuilder = new BooleanArray.Builder();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = String.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{2}'",
                catalog, dbSchema, table);

            if (columnNamePattern != null)
            {
                query = String.Concat(query, String.Format("AND column_name LIKE '{0}'", columnNamePattern));
            }

            var result = this.client.ExecuteQuery(query, parameters: null);

            foreach (var row in result)
            {
                columnNameBuilder.Append(row["column_name"].ToString());
                ordinalPositionBuilder.Append((int)(long)row["ordinal_position"]);
                remarksBuilder.Append("");
                xdbcDataTypeBuilder.AppendNull();
                string dataType = ToTypeName(row["data_type"].ToString());
                xdbcTypeNameBuilder.Append(dataType);
                xdbcColumnSizeBuilder.AppendNull();
                xdbcDecimalDigitsBuilder.AppendNull();
                xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.AppendNull();
                xdbcColumnDefBuilder.AppendNull();
                xdbcSqlDataTypeBuilder.Append((short)ToXdbcDataType(dataType));
                xdbcDatetimeSubBuilder.AppendNull();
                xdbcCharOctetLengthBuilder.AppendNull();
                xdbcIsNullableBuilder.Append(row["is_nullable"].ToString());
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.AppendNull();
                xdbcIsGeneratedcolumnBuilder.Append(row["is_generated"].ToString().ToUpper() == "YES");
                nullBitmapBuffer.Append(true);
                length++;
            }

            var dataArrays = new List<IArrowArray>
            {
                columnNameBuilder.Build(),
                ordinalPositionBuilder.Build(),
                remarksBuilder.Build(),
                xdbcDataTypeBuilder.Build(),
                xdbcTypeNameBuilder.Build(),
                xdbcColumnSizeBuilder.Build(),
                xdbcDecimalDigitsBuilder.Build(),
                xdbcNumPrecRadixBuilder.Build(),
                xdbcNullableBuilder.Build(),
                xdbcColumnDefBuilder.Build(),
                xdbcSqlDataTypeBuilder.Build(),
                xdbcDatetimeSubBuilder.Build(),
                xdbcCharOctetLengthBuilder.Build(),
                xdbcIsNullableBuilder.Build(),
                xdbcScopeCatalogBuilder.Build(),
                xdbcScopeSchemaBuilder.Build(),
                xdbcScopeTableBuilder.Build(),
                xdbcIsAutoincrementBuilder.Build(),
                xdbcIsGeneratedcolumnBuilder.Build()
            };

            return new StructArray(
                new StructType(StandardSchemas.ColumnSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StructArray GetConstraintSchema(
            GetObjectsDepth depth,
            string catalog,
            string dbSchema,
            string table,
            string columnNamePattern)
        {
            var constraintNameBuilder = new StringArray.Builder();
            var constraintTypeBuilder = new StringArray.Builder();
            var constraintColumnNamesValues = new List<IArrowArray?>();
            var constraintColumnUsageValues = new List<IArrowArray?>();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = String.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS WHERE table_name = '{2}'",
               catalog, dbSchema, table);

            var result = this.client.ExecuteQuery(query, parameters: null);

            foreach (var row in result)
            {
                string constraintName = row["constraint_name"].ToString();
                constraintNameBuilder.Append(constraintName);
                string constraintType = row["constraint_type"].ToString();
                constraintTypeBuilder.Append(constraintType);
                nullBitmapBuffer.Append(true);
                length++;

                if (depth == GetObjectsDepth.All)
                {
                    constraintColumnNamesValues.Add(GetConstraintColumnNames(
                        catalog, dbSchema, table, constraintName));
                    if (constraintType.ToUpper() == "FOREIGN KEY")
                    {
                        constraintColumnUsageValues.Add(GetConstraintsUsage(
                            catalog, dbSchema, table, constraintName));
                    }
                    else
                    {
                        constraintColumnUsageValues.Add(null);
                    }
                }
                else
                {
                    constraintColumnNamesValues.Add(null);
                    constraintColumnUsageValues.Add(null);
                }
            }

            var dataArrays = new List<IArrowArray>
            {
                constraintNameBuilder.Build(),
                constraintTypeBuilder.Build(),
                CreateNestedListArray(constraintColumnNamesValues, StringType.Default),
                CreateNestedListArray(constraintColumnUsageValues, new StructType(StandardSchemas.UsageSchema))
            };

            return new StructArray(
                new StructType(StandardSchemas.ConstraintSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StringArray GetConstraintColumnNames(
            string catalog,
            string dbSchema,
            string table,
            string constraintName)
        {
            string query = String.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE table_name = '{2}' AND constraint_name = '{3}' ORDER BY ordinal_position",
               catalog, dbSchema, table, constraintName);

            var constraintColumnNamesBuilder = new StringArray.Builder();

            var result = this.client.ExecuteQuery(query, parameters: null);

            foreach (var row in result)
            {
                constraintColumnNamesBuilder.Append(row["column_name"].ToString());
            }

            return constraintColumnNamesBuilder.Build();
        }

        private StructArray GetConstraintsUsage(
            string catalog,
            string dbSchema,
            string table,
            string constraintName)
        {
            var constraintFkCatalogBuilder = new StringArray.Builder();
            var constraintFkDbSchemaBuilder = new StringArray.Builder();
            var constraintFkTableBuilder = new StringArray.Builder();
            var constraintFkColumnNameBuilder = new StringArray.Builder();
            var nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;

            string query = String.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE WHERE table_name = '{2}' AND constraint_name = '{3}'",
               catalog, dbSchema, table, constraintName);

            var result = this.client.ExecuteQuery(query, parameters: null);

            foreach (var row in result)
            {
                constraintFkCatalogBuilder.Append(row["constraint_catalog"].ToString());
                constraintFkDbSchemaBuilder.Append(row["constraint_schema"].ToString());
                constraintFkTableBuilder.Append(row["table_name"].ToString());
                constraintFkColumnNameBuilder.Append(row["column_name"].ToString());
                nullBitmapBuffer.Append(true);
                length++;
            }

            var dataArrays = new List<IArrowArray>
            {
                constraintFkCatalogBuilder.Build(),
                constraintFkDbSchemaBuilder.Build(),
                constraintFkTableBuilder.Build(),
                constraintFkColumnNameBuilder.Build()
            };

            return new StructArray(
                new StructType(StandardSchemas.UsageSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private string PatternToRegexp(string pattern)
        {
            if (pattern == null)
                return ".*";

            var builder = new StringBuilder("(?i)^");
            var covertedPattern = pattern.Replace("_", ".").Replace("%", ".*");
            builder.Append(covertedPattern);
            builder.Append("$");

            return builder.ToString();
        }

        private string ToTypeName(string type)
        {
            int index = Math.Min(type.IndexOf("("), type.IndexOf("<"));
            string dataType = index == -1 ? type : type.Substring(0, index);
            return dataType;
        }

        private XdbcDataType ToXdbcDataType(string type)
        {
            switch (type)
            {
                case "INTEGER" or "INT64":
                    return XdbcDataType.XdbcDataType_XDBC_INTEGER;
                case "FLOAT" or "FLOAT64":
                    return XdbcDataType.XdbcDataType_XDBC_FLOAT;
                case "BOOL" or "BOOLEAN":
                    return XdbcDataType.XdbcDataType_XDBC_BIT;
                case "STRING":
                    return XdbcDataType.XdbcDataType_XDBC_VARCHAR;
                case "BYTES":
                    return XdbcDataType.XdbcDataType_XDBC_BINARY;
                case "DATETIME":
                    return XdbcDataType.XdbcDataType_XDBC_DATETIME;
                case "TIMESTAMP":
                    return XdbcDataType.XdbcDataType_XDBC_TIMESTAMP;
                case "TIME":
                    return XdbcDataType.XdbcDataType_XDBC_TIME;
                case "DATE":
                    return XdbcDataType.XdbcDataType_XDBC_DATE;
                case "RECORD" or "STRUCT":
                    return XdbcDataType.XdbcDataType_XDBC_VARBINARY;
                case "NUMERIC" or "DECIMAL" or "BIGNUMERIC" or "BIGDECIMAL":
                    return XdbcDataType.XdbcDataType_XDBC_NUMERIC;

                default:
                    return XdbcDataType.XdbcDataType_XDBC_UNKNOWN_TYPE;
            }
        }

        public override Schema GetTableSchema(string catalog, string dbSchema, string tableName)
        {
            string query = String.Format("SELECT * FROM `{0}`.`{1}`.INFORMATION_SCHEMA.COLUMNS WHERE table_name = '{2}'",
                catalog, dbSchema, tableName);

            // not sure if statement should be used

            var result = this.client.ExecuteQuery(query, parameters: null);

            List<Field> fields = new List<Field>();

            foreach (var row in result)
            {
                fields.Add(DescToField(row));
            }

            return new Schema(fields, null);
        }

        private Field DescToField(BigQueryRow row)
        {
            Dictionary<string, string> metaData = new Dictionary<string, string>();
            metaData.Add("PRIMARY_KEY", "");
            metaData.Add("ORDINAL_POSITION", row["ordinal_position"].ToString());
            metaData.Add("DATA_TYPE", row["data_type"].ToString());

            var fieldBuilder = SchemaFieldGenerator(row["column_name"].ToString().ToLower(), row["data_type"].ToString());
            fieldBuilder.Metadata(metaData);

            if (!row["is_nullable"].ToString().Equals("YES", StringComparison.OrdinalIgnoreCase))
            {
                fieldBuilder.Nullable(false);
            }

            fieldBuilder.Name(row["column_name"].ToString().ToLower());

            return fieldBuilder.Build();
        }

        private Field.Builder SchemaFieldGenerator(string name, string type)
        {
            var fieldBuilder = new Field.Builder();
            fieldBuilder.Name(name);

            int index = type.IndexOf("(");
            index = index == -1 ? type.IndexOf("<") : Math.Min(index, type.IndexOf("<"));
            string dataType = index == -1 ? type : type.Substring(0, index);

            switch (dataType)
            {
                case "INTEGER" or "INT64":
                    return fieldBuilder.DataType(Int64Type.Default);
                case "FLOAT" or "FLOAT64":
                    return fieldBuilder.DataType(DoubleType.Default);
                case "BOOL" or "BOOLEAN":
                    return fieldBuilder.DataType(BooleanType.Default);
                case "STRING":
                    return fieldBuilder.DataType(StringType.Default);
                case "BYTES":
                    return fieldBuilder.DataType(BinaryType.Default);
                case "DATETIME":
                    return fieldBuilder.DataType(TimestampType.Default);
                case "TIMESTAMP":
                    return fieldBuilder.DataType(TimestampType.Default);
                case "TIME":
                    return fieldBuilder.DataType(Time64Type.Default);
                case "DATE":
                    return fieldBuilder.DataType(Date64Type.Default);
                case "RECORD" or "STRUCT":
                    string fieldRecords = type.Substring(index + 1);
                    fieldRecords = fieldRecords.Remove(fieldRecords.Length - 1);
                    List<Field> nestedFields = new List<Field>();

                    foreach (string record in fieldRecords.Split(','))
                    {
                        string fieldRecord = record.Trim();
                        string fieldName = fieldRecord.Split(' ')[0];
                        string fieldType = fieldRecord.Split(' ')[1];
                        nestedFields.Add(SchemaFieldGenerator(fieldName, fieldType).Build());
                    }

                    return fieldBuilder.DataType(new StructType(nestedFields));
                case "NUMERIC" or "DECIMAL":
                    return fieldBuilder.DataType(new Decimal128Type(38, 9));
                case "BIGNUMERIC" or "BIGDECIMAL":
                    return fieldBuilder.DataType(new Decimal256Type(76, 38));
                default: throw new InvalidOperationException();
            }
        }

        public override IArrowArrayStream GetTableTypes()
        {
            var tableTypesBuilder = new StringArray.Builder();
            tableTypesBuilder.AppendRange(new string[] { "BASE TABLE", "VIEW" });

            var dataArrays = new List<IArrowArray>
            {
                tableTypesBuilder.Build()
            };

            return new BigQueryInfoArrowStream(StandardSchemas.TableTypesSchema, dataArrays, 1);
        }

        public ListArray CreateNestedListArray(List<IArrowArray> arrayList, IArrowType dataType)
        {
            var valueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
            var validityBufferBuilder = new ArrowBuffer.BitmapBuilder();
            var arrayDataList = new List<ArrayData>(arrayList.Count);
            int length = 0;
            int nullCount = 0;

            foreach (var array in arrayList)
            {
                if (array == null)
                {
                    valueOffsetsBufferBuilder.Append(length);
                    validityBufferBuilder.Append(false);
                    nullCount++;
                }
                else
                {
                    valueOffsetsBufferBuilder.Append(length);
                    validityBufferBuilder.Append(true);
                    arrayDataList.Add(array.Data);
                    length += array.Length;
                }
            }

            ArrowBuffer validityBuffer = nullCount > 0
                ? validityBufferBuilder.Build() : ArrowBuffer.Empty;

            // ArrowArrayConcatenator.Concatenate is still internal, replace the concatenate in the future
            IArrowArray value = ArrowArrayFactory.BuildArray(ArrayDataConcatenator.Concatenate(arrayDataList));

            valueOffsetsBufferBuilder.Append(length);

            return new ListArray(new ListType(dataType), arrayList.Count,
                    valueOffsetsBufferBuilder.Build(), value,
                    validityBuffer, nullCount, 0);
        }

        public IArrowArrayStream CreateArrowArrayStream(Schema schema, IEnumerable<IArrowArray> data, int length)
        {
            RecordBatch recordBatch = new RecordBatch(schema, data, length);
            var stream = new MemoryStream();
            var writer = new ArrowStreamWriter(stream, schema);
            writer.WriteRecordBatch(recordBatch);
            writer.WriteEnd();
            var streamReader = new ArrowStreamReader(stream);

            return streamReader;
        }

        public override AdbcStatement CreateStatement()
        {
            if (this.client == null || this.credential == null) { throw new InvalidOperationException(); }
            var statement = new BigQueryStatement(this.client, this.credential);
            statement.Options = ValidateOptions();
            return statement;
        }

        private IReadOnlyDictionary<string, string> ValidateOptions()
        {
            Dictionary<string, string> options = new Dictionary<string, string>();
            foreach (var keyValuePair in this.properties)
            {
                if (keyValuePair.Key == "AllowLargeResults")
                {
                    options[keyValuePair.Key] = keyValuePair.Value;
                }
                if (keyValuePair.Key == "UseLegacySQL")
                {
                    options[keyValuePair.Key] = keyValuePair.Value;
                }
            }
            return new ReadOnlyDictionary<string, string>(options);
        }

        public override void Dispose()
        {
            this.client?.Dispose();
            this.client = null;
        }

        public enum XdbcDataType
        {
            XdbcDataType_XDBC_UNKNOWN_TYPE = 0,
            XdbcDataType_XDBC_CHAR = 1,
            XdbcDataType_XDBC_NUMERIC = 2,
            XdbcDataType_XDBC_DECIMAL = 3,
            XdbcDataType_XDBC_INTEGER = 4,
            XdbcDataType_XDBC_SMALLINT = 5,
            XdbcDataType_XDBC_FLOAT = 6,
            XdbcDataType_XDBC_REAL = 7,
            XdbcDataType_XDBC_DOUBLE = 8,
            XdbcDataType_XDBC_DATETIME = 9,
            XdbcDataType_XDBC_INTERVAL = 10,
            XdbcDataType_XDBC_VARCHAR = 12,
            XdbcDataType_XDBC_DATE = 91,
            XdbcDataType_XDBC_TIME = 92,
            XdbcDataType_XDBC_TIMESTAMP = 93,
            XdbcDataType_XDBC_LONGVARCHAR = -1,
            XdbcDataType_XDBC_BINARY = -2,
            XdbcDataType_XDBC_VARBINARY = -3,
            XdbcDataType_XDBC_LONGVARBINARY = -4,
            XdbcDataType_XDBC_BIGINT = -5,
            XdbcDataType_XDBC_TINYINT = -6,
            XdbcDataType_XDBC_BIT = -7,
            XdbcDataType_XDBC_WCHAR = -8,
            XdbcDataType_XDBC_WVARCHAR = -9,
        }
    }
}
