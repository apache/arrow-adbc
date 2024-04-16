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
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;

using Apache.Arrow.Adbc.Drivers.Apache.Thrift;
using System.Diagnostics;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    public class SparkConnection : HiveServer2Connection
    {
        const string userAgent = "MicrosoftSparkODBCDriver/2.7.6.1014";

        readonly IReadOnlyList<AdbcInfoCode> infoSupportedCodes = new List<AdbcInfoCode> {
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName
        };

        const string infoDriverName = "ADBC Spark Driver";
        const string infoDriverVersion = "1.0.0";
        const string infoVendorName = "Spark";
        const string infoDriverArrowVersion = "1.0.0";

        internal static TSparkGetDirectResults sparkGetDirectResults = new TSparkGetDirectResults(1000);

        internal static readonly Dictionary<string, string> timestampConfig = new Dictionary<string, string>
        {
            { "spark.thriftserver.arrowBasedRowSet.timestampAsString", "false" }
        };

        private enum ColumnTypeId
        {
            BOOLEAN_TYPE = 16,
            TINYINT_TYPE = -6,
            SMALLINT_TYPE = 5,
            INT_TYPE = 4,
            BIGINT_TYPE = -5,
            FLOAT_TYPE = 6,
            DOUBLE_TYPE = 8,
            STRING_TYPE = 12,
            TIMESTAMP_TYPE = 93,
            BINARY_TYPE = -2,
            ARRAY_TYPE = 2003,
            MAP_TYPE = 2000,
            STRUCT_TYPE = 2002,
            DECIMAL_TYPE = 3,
            DATE_TYPE = 91,
            CHAR_TYPE = 1,
        }

        public SparkConnection() : this(null)
        {

        }

        internal SparkConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
        }

        protected override TProtocol CreateProtocol()
        {
            Trace.TraceError($"create protocol with {properties.Count} properties.");

            foreach (var property in properties.Keys)
            {
                Trace.TraceError($"key = {property} value = {properties[property]}");
            }

            string hostName = properties["hostname"];
            string path = properties["path"];
            string token;

            if (properties.ContainsKey("token"))
                token = properties["token"];
            else
                token = properties["password"];

            string uri = "https://" + hostName + "/" + path;

            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new Uri(uri);
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
            httpClient.DefaultRequestHeaders.AcceptEncoding.Clear();
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
            httpClient.DefaultRequestHeaders.ExpectContinue = false;

            TConfiguration config = new TConfiguration();

            ThriftHttpTransport transport = new ThriftHttpTransport(httpClient, config);
            // can switch to the one below if want to use the experimental one with IPeekableTransport
            // ThriftHttpTransport transport = new ThriftHttpTransport(httpClient, config);
            transport.OpenAsync(CancellationToken.None).Wait();
            return new TBinaryProtocol(transport);
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            return new TOpenSessionReq(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7)
            {
                CanUseMultipleCatalogs = true,
                Configuration = timestampConfig,
            };
        }

        public override AdbcStatement CreateStatement()
        {
            return new SparkStatement(this);
        }

        public override void Dispose()
        {
            /*
            if (this.client != null)
            {
                TCloseSessionReq r6 = new TCloseSessionReq(this.sessionHandle);
                this.client.CloseSession(r6).Wait();

                this.transport.Close();
                this.client.Dispose();

                this.transport = null;
                this.client = null;
            }
            */
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
                new int[] { 0, 1, 2, 3, 4, 5 },
                UnionMode.Dense);

            if (codes.Count == 0)
            {
                codes = new List<AdbcInfoCode>(infoSupportedCodes);
            }

            UInt32Array.Builder infoNameBuilder = new UInt32Array.Builder();
            ArrowBuffer.Builder<byte> typeBuilder = new ArrowBuffer.Builder<byte>();
            ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>();
            StringArray.Builder stringInfoBuilder = new StringArray.Builder();
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

            StructType entryType = new StructType(
                new List<Field>(){
                    new Field("key", Int32Type.Default, false),
                    new Field("value", Int32Type.Default, true)});

            StructArray entriesDataArray = new StructArray(entryType, 0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            List<IArrowArray> childrenArrays = new List<IArrowArray>()
            {
                stringInfoBuilder.Build(),
                new BooleanArray.Builder().Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                CreateNestedListArray(new List<IArrowArray?>(){ entriesDataArray }, entryType)
            };

            DenseUnionArray infoValue = new DenseUnionArray(infoUnionType, arrayLength, childrenArrays, typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            List<IArrowArray> dataArrays = new List<IArrowArray>
            {
                infoNameBuilder.Build(),
                infoValue
            };

            return new SparkInfoArrowStream(StandardSchemas.GetInfoSchema, dataArrays);

        }

        public override IArrowArrayStream GetInfo(List<int> codes) => base.GetInfo(codes);

        public override IArrowArrayStream GetTableTypes()
        {
            StringArray.Builder tableTypesBuilder = new StringArray.Builder();
            tableTypesBuilder.AppendRange(new string[] { "BASE TABLE", "VIEW" });

            List<IArrowArray> dataArrays = new List<IArrowArray>
            {
                tableTypesBuilder.Build()
            };

            return new SparkInfoArrowStream(StandardSchemas.TableTypesSchema, dataArrays);
        }

        public override Schema GetTableSchema(string catalog, string dbSchema, string tableName)
        {
            TGetColumnsReq getColumnsReq = new TGetColumnsReq(this.sessionHandle);
            getColumnsReq.CatalogName = catalog;
            getColumnsReq.SchemaName = dbSchema;
            getColumnsReq.TableName = tableName;
            getColumnsReq.GetDirectResults = sparkGetDirectResults;

            var columnsResponse = this.client.GetColumns(getColumnsReq).Result;
            if (columnsResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new Exception(columnsResponse.Status.ErrorMessage);
            }

            var result = columnsResponse.DirectResults;
            var resultSchema = result.ResultSetMetadata.ArrowSchema;
            var columns = result.ResultSet.Results.Columns;
            var rowCount = columns[3].StringVal.Values.Length;

            Field[] fields = new Field[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                string columnName = columns[3].StringVal.Values.GetString(i);
                int? columnType = columns[4].I32Val.Values.GetValue(i);
                string typeName = columns[5].StringVal.Values.GetString(i);
                bool nullable = columns[10].I32Val.Values.GetValue(i) == 1;
                IArrowType dataType = SparkConnection.GetArrowType((ColumnTypeId)columnType, typeName);
                fields[i] = new Field(columnName, dataType, nullable);
            }
            return new Schema(fields, null);
        }
        private static IReadOnlyList<int> ConvertSpanToReadOnlyList(Int32Array span)
        {
            // Initialize a list with the capacity equal to the length of the span
            // to avoid resizing during the addition of elements
            List<int> list = new List<int>(span.Length);

            // Copy elements from the span to the list
            foreach (int item in span)
            {
                list.Add(item);
            }

            // Return the list as IReadOnlyList<int>
            return list;
        }

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string catalogPattern, string dbSchemaPattern, string tableNamePattern, List<string> tableTypes, string columnNamePattern)
        {
            Trace.TraceError($"getting objects with depth={depth.ToString()}, catalog = {catalogPattern}, dbschema = {dbSchemaPattern}, tablename = {tableNamePattern}");

            Dictionary<string, Dictionary<string, Dictionary<string, TableInfoPair>>> catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfoPair>>>();
            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
            {
                TGetCatalogsReq getCatalogsReq = new TGetCatalogsReq(this.sessionHandle);
                getCatalogsReq.GetDirectResults = sparkGetDirectResults;

                TGetCatalogsResp getCatalogsResp = this.client.GetCatalogs(getCatalogsReq).Result;
                if (getCatalogsResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(getCatalogsResp.Status.ErrorMessage);
                }

                string catalogRegexp = PatternToRegEx(catalogPattern);
                TRowSet resp = getCatalogsResp.DirectResults.ResultSet.Results;
                IReadOnlyList<string> list = resp.Columns[0].StringVal.Values;
                for (int i = 0; i < list.Count; i++)
                {
                    string col = list[i];
                    string catalog = col;

                    if (Regex.IsMatch(catalog, catalogRegexp, RegexOptions.IgnoreCase))
                    {
                        catalogMap.Add(catalog, new Dictionary<string, Dictionary<string, TableInfoPair>>());
                    }
                }
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.DbSchemas)
            {
                TGetSchemasReq getSchemasReq = new TGetSchemasReq(this.sessionHandle);
                getSchemasReq.CatalogName = catalogPattern;
                getSchemasReq.SchemaName = dbSchemaPattern;
                getSchemasReq.GetDirectResults = sparkGetDirectResults;

                TGetSchemasResp getSchemasResp = this.client.GetSchemas(getSchemasReq).Result;
                if (getSchemasResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(getSchemasResp.Status.ErrorMessage);
                }
                TRowSet resp = getSchemasResp.DirectResults.ResultSet.Results;

                IReadOnlyList<string> catalogList = resp.Columns[1].StringVal.Values;
                IReadOnlyList<string> schemaList = resp.Columns[0].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    // It seems Spark sometimes returns empty string for catalog on some schema (temporary tables).
                    catalogMap.GetValueOrDefault(catalog)?.Add(schemaDb, new Dictionary<string, TableInfoPair>());
                }
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Tables)
            {
                TGetTablesReq getTablesReq = new TGetTablesReq(this.sessionHandle);
                getTablesReq.CatalogName = catalogPattern;
                getTablesReq.SchemaName = dbSchemaPattern;
                getTablesReq.TableName = tableNamePattern;
                getTablesReq.GetDirectResults = sparkGetDirectResults;

                TGetTablesResp getTablesResp = this.client.GetTables(getTablesReq).Result;
                if (getTablesResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(getTablesResp.Status.ErrorMessage);
                }
                TRowSet resp = getTablesResp.DirectResults.ResultSet.Results;

                IReadOnlyList<string> catalogList = resp.Columns[0].StringVal.Values;
                IReadOnlyList<string> schemaList = resp.Columns[1].StringVal.Values;
                IReadOnlyList<string> tableList = resp.Columns[2].StringVal.Values;
                IReadOnlyList<string> tableTypeList = resp.Columns[3].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    string tableName = tableList[i];
                    string tableType = tableTypeList[i];
                    TableInfoPair tableInfo = new TableInfoPair();
                    tableInfo.Type = tableType;
                    tableInfo.Columns = new List<string>();
                    tableInfo.ColType = new List<int>();
                    catalogMap.GetValueOrDefault(catalog).GetValueOrDefault(schemaDb).Add(tableName, tableInfo);
                }
            }

            if (depth == GetObjectsDepth.All)
            {
                TGetColumnsReq columnsReq = new TGetColumnsReq(this.sessionHandle);
                columnsReq.CatalogName = catalogPattern;
                columnsReq.SchemaName = dbSchemaPattern;
                columnsReq.TableName = tableNamePattern;
                columnsReq.GetDirectResults = sparkGetDirectResults;

                if (!string.IsNullOrEmpty(columnNamePattern))
                    columnsReq.ColumnName = columnNamePattern;

                var columnsResponse = this.client.GetColumns(columnsReq).Result;
                if (columnsResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(columnsResponse.Status.ErrorMessage);
                }

                TRowSet resp = columnsResponse.DirectResults.ResultSet.Results;

                IReadOnlyList<string> catalogList = resp.Columns[0].StringVal.Values;
                IReadOnlyList<string> schemaList = resp.Columns[1].StringVal.Values;
                IReadOnlyList<string> tableList = resp.Columns[2].StringVal.Values;
                IReadOnlyList<string> columnList = resp.Columns[3].StringVal.Values;
                IReadOnlyList<int> columnTypeList = ConvertSpanToReadOnlyList(resp.Columns[4].I32Val.Values);

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    string tableName = tableList[i];
                    string column = columnList[i];
                    int colType = columnTypeList[i];
                    TableInfoPair tableInfo = catalogMap.GetValueOrDefault(catalog).GetValueOrDefault(schemaDb).GetValueOrDefault(tableName);
                    tableInfo.Columns.Add(column);
                    tableInfo.ColType.Add(colType);
                }
            }

            StringArray.Builder catalogNameBuilder = new StringArray.Builder();
            List<IArrowArray?> catalogDbSchemasValues = new List<IArrowArray?>();


            foreach (KeyValuePair<string, Dictionary<string, Dictionary<string, TableInfoPair>>> catalogEntry in catalogMap)
            {
                catalogNameBuilder.Append(catalogEntry.Key);

                if (depth == GetObjectsDepth.Catalogs)
                {
                    catalogDbSchemasValues.Add(null);
                }
                else
                {
                    catalogDbSchemasValues.Add(GetDbSchemas(
                                depth, catalogEntry.Value));
                }

            }

            List<IArrowArray> dataArrays = new List<IArrowArray>
            {
                catalogNameBuilder.Build(),
                CreateNestedListArray(catalogDbSchemasValues, new StructType(StandardSchemas.DbSchemaSchema)),
            };
            return new SparkInfoArrowStream(StandardSchemas.GetObjectsSchema, dataArrays);
        }

        private static IArrowType GetArrowType(ColumnTypeId columnTypeId, string typeName)
        {
            switch (columnTypeId)
            {
                case ColumnTypeId.BOOLEAN_TYPE:
                    return BooleanType.Default;
                case ColumnTypeId.TINYINT_TYPE:
                    return Int8Type.Default;
                case ColumnTypeId.SMALLINT_TYPE:
                    return Int16Type.Default;
                case ColumnTypeId.INT_TYPE:
                    return Int32Type.Default;
                case ColumnTypeId.BIGINT_TYPE:
                    return Int64Type.Default;
                case ColumnTypeId.FLOAT_TYPE:
                    return FloatType.Default;
                case ColumnTypeId.DOUBLE_TYPE:
                    return DoubleType.Default;
                case ColumnTypeId.STRING_TYPE:
                    return StringType.Default;
                case ColumnTypeId.TIMESTAMP_TYPE:
                    return new TimestampType(TimeUnit.Microsecond, timezone: (string)null);
                case ColumnTypeId.BINARY_TYPE:
                    return BinaryType.Default;
                case ColumnTypeId.DATE_TYPE:
                    return Date32Type.Default;
                case ColumnTypeId.CHAR_TYPE:
                    return StringType.Default;
                case ColumnTypeId.DECIMAL_TYPE:
                    // TODO: Parse typeName for precision and scale, because not available in other metadata.
                    return new Decimal128Type(38, 38);
                case ColumnTypeId.ARRAY_TYPE:
                case ColumnTypeId.MAP_TYPE:
                case ColumnTypeId.STRUCT_TYPE:
                    return StringType.Default;
                default:
                    throw new NotImplementedException($"Column type id: {columnTypeId} is not supported.");
            }
        }

        private StructArray GetDbSchemas(
            GetObjectsDepth depth,
            Dictionary<string, Dictionary<string, TableInfoPair>> schemaMap)
        {
            StringArray.Builder dbSchemaNameBuilder = new StringArray.Builder();
            List<IArrowArray?> dbSchemaTablesValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            foreach (KeyValuePair<string, Dictionary<string, TableInfoPair>> schemaEntry in schemaMap)
            {

                dbSchemaNameBuilder.Append(schemaEntry.Key);
                length++;
                nullBitmapBuffer.Append(true);

                if (depth == GetObjectsDepth.DbSchemas)
                {
                    dbSchemaTablesValues.Add(null);
                }
                else
                {
                    dbSchemaTablesValues.Add(GetTableSchemas(
                        depth, schemaEntry.Value));
                }

            }


            List<IArrowArray> dataArrays = new List<IArrowArray>
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
            Dictionary<string, TableInfoPair> tableMap)
        {
            StringArray.Builder tableNameBuilder = new StringArray.Builder();
            StringArray.Builder tableTypeBuilder = new StringArray.Builder();
            List<IArrowArray?> tableColumnsValues = new List<IArrowArray?>();
            List<IArrowArray?> tableConstraintsValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            foreach (KeyValuePair<string, TableInfoPair> tableEntry in tableMap)
            {
                tableNameBuilder.Append(tableEntry.Key);
                tableTypeBuilder.Append(tableEntry.Value.Type);
                nullBitmapBuffer.Append(true);
                length++;


                tableConstraintsValues.Add(null);


                if (depth == GetObjectsDepth.Tables)
                {
                    tableColumnsValues.Add(null);
                }
                else
                {
                    tableColumnsValues.Add(GetColumnSchema(tableEntry.Value.Columns, tableEntry.Value.ColType));
                }
            }


            List<IArrowArray> dataArrays = new List<IArrowArray>
            {
                tableNameBuilder.Build(),
                tableTypeBuilder.Build(),
                CreateNestedListArray(tableColumnsValues, new StructType(StandardSchemas.ColumnSchema)),
                CreateNestedListArray(tableConstraintsValues, new StructType(StandardSchemas.ConstraintSchema))
            };

            return new StructArray(
                new StructType(StandardSchemas.TableSchema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private StructArray GetColumnSchema(
            List<string> columns, List<int> colTypes)
        {
            StringArray.Builder columnNameBuilder = new StringArray.Builder();
            Int32Array.Builder ordinalPositionBuilder = new Int32Array.Builder();
            StringArray.Builder remarksBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcDataTypeBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcTypeNameBuilder = new StringArray.Builder();
            Int32Array.Builder xdbcColumnSizeBuilder = new Int32Array.Builder();
            Int16Array.Builder xdbcDecimalDigitsBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNullableBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcColumnDefBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcDatetimeSubBuilder = new Int16Array.Builder();
            Int32Array.Builder xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            StringArray.Builder xdbcIsNullableBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeCatalogBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeSchemaBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeTableBuilder = new StringArray.Builder();
            BooleanArray.Builder xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
            BooleanArray.Builder xdbcIsGeneratedcolumnBuilder = new BooleanArray.Builder();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            for (int i = 0; i < columns.Count; i++)
            {
                columnNameBuilder.Append(columns[i]);
                ordinalPositionBuilder.Append((int)colTypes[i]);
                remarksBuilder.Append("");



                xdbcColumnSizeBuilder.AppendNull();
                xdbcDecimalDigitsBuilder.AppendNull();


                xdbcDataTypeBuilder.AppendNull();
                xdbcTypeNameBuilder.Append("");
                xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.AppendNull();
                xdbcColumnDefBuilder.AppendNull();
                xdbcSqlDataTypeBuilder.Append((short)colTypes[i]);
                xdbcDatetimeSubBuilder.AppendNull();
                xdbcCharOctetLengthBuilder.AppendNull();
                xdbcIsNullableBuilder.Append("true");
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.AppendNull();
                xdbcIsGeneratedcolumnBuilder.Append(true);
                nullBitmapBuffer.Append(true);
                length++;
            }

            List<IArrowArray> dataArrays = new List<IArrowArray>
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

        private ListArray CreateNestedListArray(List<IArrowArray?> arrayList, IArrowType dataType)
        {
            ArrowBuffer.Builder<int> valueOffsetsBufferBuilder = new ArrowBuffer.Builder<int>();
            ArrowBuffer.BitmapBuilder validityBufferBuilder = new ArrowBuffer.BitmapBuilder();
            List<ArrayData> arrayDataList = new List<ArrayData>(arrayList.Count);
            int length = 0;
            int nullCount = 0;

            foreach (IArrowArray? array in arrayList)
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

            ArrayData? data = ArrayDataConcatenator.Concatenate(arrayDataList);

            if (data == null)
            {
                EmptyArrayCreationVisitor visitor = new EmptyArrayCreationVisitor();
                dataType.Accept(visitor);
                data = visitor.Result;
            }

            IArrowArray value = ArrowArrayFactory.BuildArray(data);

            valueOffsetsBufferBuilder.Append(length);

            return new ListArray(new ListType(dataType), arrayList.Count,
                    valueOffsetsBufferBuilder.Build(), value,
                    validityBuffer, nullCount, 0);
        }

        private class EmptyArrayCreationVisitor :
            IArrowTypeVisitor<BooleanType>,
            IArrowTypeVisitor<FixedWidthType>,
            IArrowTypeVisitor<BinaryType>,
            IArrowTypeVisitor<StringType>,
            IArrowTypeVisitor<ListType>,
            IArrowTypeVisitor<FixedSizeListType>,
            IArrowTypeVisitor<StructType>,
            IArrowTypeVisitor<MapType>
        {
            public ArrayData? Result { get; private set; }

            public void Visit(BooleanType type)
            {
                Result = new BooleanArray.Builder().Build().Data;
            }

            public void Visit(FixedWidthType type)
            {
                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty, ArrowBuffer.Empty });
            }

            public void Visit(BinaryType type)
            {
                Result = new BinaryArray.Builder().Build().Data;
            }

            public void Visit(StringType type)
            {
                Result = new StringArray.Builder().Build().Data;
            }

            public void Visit(ListType type)
            {
                type.ValueDataType.Accept(this);
                ArrayData? child = Result;

                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty, MakeInt0Buffer() }, new[] { child });
            }

            public void Visit(FixedSizeListType type)
            {
                type.ValueDataType.Accept(this);
                ArrayData? child = Result;

                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty }, new[] { child });
            }

            public void Visit(StructType type)
            {
                ArrayData?[] children = new ArrayData[type.Fields.Count];
                for (int i = 0; i < type.Fields.Count; i++)
                {
                    type.Fields[i].DataType.Accept(this);
                    children[i] = Result;
                }

                Result = new ArrayData(type, 0, 0, 0, new[] { ArrowBuffer.Empty }, children);
            }

            public void Visit(MapType type)
            {
                Result = new MapArray.Builder(type).Build().Data;
            }

            public void Visit(IArrowType type)
            {
                throw new NotImplementedException($"EmptyArrayCreationVisitor for {type.Name} is not supported yet.");
            }

            private static ArrowBuffer MakeInt0Buffer()
            {
                ArrowBuffer.Builder<int> builder = new ArrowBuffer.Builder<int>();
                builder.Append(0);
                return builder.Build();
            }
        }

        private string PatternToRegEx(string pattern)
        {
            if (pattern == null)
                return ".*";

            StringBuilder builder = new StringBuilder("(?i)^");
            string convertedPattern = pattern.Replace("_", ".").Replace("%", ".*");
            builder.Append(convertedPattern);
            builder.Append("$");

            return builder.ToString();
        }
    }

    public struct TableInfoPair
    {
        public string Type { get; set; }

        public List<string> Columns { get; set; }

        public List<int> ColType { get; set; }
    }

    internal class SparkInfoArrowStream : IArrowArrayStream
    {
        private Schema schema;
        private RecordBatch? batch;

        public SparkInfoArrowStream(Schema schema, List<IArrowArray> data)
        {
            this.schema = schema;
            this.batch = new RecordBatch(schema, data, data[0].Length);
        }

        public Schema Schema { get { return this.schema; } }

        public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
        {
            RecordBatch? batch = this.batch;
            this.batch = null;
            return new ValueTask<RecordBatch?>(batch);
        }

        public void Dispose()
        {
            this.batch?.Dispose();
            this.batch = null;
        }
    }
}
