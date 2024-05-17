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
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Thrift;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    public class SparkConnection : HiveServer2Connection
    {
        const string UserAgent = "MicrosoftSparkODBCDriver/2.7.6.1014";

        readonly AdbcInfoCode[] infoSupportedCodes = new[] {
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName,
            AdbcInfoCode.VendorSql,
            AdbcInfoCode.VendorVersion,
        };

        const string ProductVersionDefault = "1.0.0";
        const string InfoDriverName = "ADBC Spark Driver";
        const string InfoDriverArrowVersion = "1.0.0";
        const bool InfoVendorSql = true;
        const int DecimalPrecisionDefault = 10;
        const int DecimalScaleDefault = 0;

        private readonly Lazy<string> _productVersion;

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

        internal SparkConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
            _productVersion = new Lazy<string>(() => GetProductVersion(), LazyThreadSafetyMode.PublicationOnly);
        }

        protected string ProductVersion => _productVersion.Value;

        protected override async ValueTask<TProtocol> CreateProtocolAsync()
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

            HttpClient httpClient = new HttpClient();
            httpClient.BaseAddress = new UriBuilder(Uri.UriSchemeHttps, hostName, -1, path).Uri;
            httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", token);
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(UserAgent);
            httpClient.DefaultRequestHeaders.AcceptEncoding.Clear();
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
            httpClient.DefaultRequestHeaders.ExpectContinue = false;

            TConfiguration config = new TConfiguration();

            ThriftHttpTransport transport = new ThriftHttpTransport(httpClient, config);
            // can switch to the one below if want to use the experimental one with IPeekableTransport
            // ThriftHttpTransport transport = new ThriftHttpTransport(httpClient, config);
            await transport.OpenAsync(CancellationToken.None);
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

        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            const int strValTypeID = 0;
            const int boolValTypeId = 1;

            UnionType infoUnionType = new UnionType(
                new Field[]
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
                                new Field[]
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
                codes = infoSupportedCodes;
            }

            UInt32Array.Builder infoNameBuilder = new UInt32Array.Builder();
            ArrowBuffer.Builder<byte> typeBuilder = new ArrowBuffer.Builder<byte>();
            ArrowBuffer.Builder<int> offsetBuilder = new ArrowBuffer.Builder<int>();
            StringArray.Builder stringInfoBuilder = new StringArray.Builder();
            BooleanArray.Builder booleanInfoBuilder = new BooleanArray.Builder();

            int nullCount = 0;
            int arrayLength = codes.Count;
            int offset = 0;

            foreach (AdbcInfoCode code in codes)
            {
                switch (code)
                {
                    case AdbcInfoCode.DriverName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(InfoDriverName);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.DriverVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(ProductVersion);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.DriverArrowVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.Append(InfoDriverArrowVersion);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.VendorName:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        string vendorName = VendorName;
                        stringInfoBuilder.Append(vendorName);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.VendorVersion:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        string? vendorVersion = VendorVersion;
                        stringInfoBuilder.Append(vendorVersion);
                        booleanInfoBuilder.AppendNull();
                        break;
                    case AdbcInfoCode.VendorSql:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(boolValTypeId);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.Append(InfoVendorSql);
                        break;
                    default:
                        infoNameBuilder.Append((UInt32)code);
                        typeBuilder.Append(strValTypeID);
                        offsetBuilder.Append(offset++);
                        stringInfoBuilder.AppendNull();
                        booleanInfoBuilder.AppendNull();
                        nullCount++;
                        break;
                }
            }

            StructType entryType = new StructType(
                new Field[] {
                    new Field("key", Int32Type.Default, false),
                    new Field("value", Int32Type.Default, true)});

            StructArray entriesDataArray = new StructArray(entryType, 0,
                new[] { new Int32Array.Builder().Build(), new Int32Array.Builder().Build() },
                new ArrowBuffer.BitmapBuilder().Build());

            IArrowArray[] childrenArrays = new IArrowArray[]
            {
                stringInfoBuilder.Build(),
                booleanInfoBuilder.Build(),
                new Int64Array.Builder().Build(),
                new Int32Array.Builder().Build(),
                new ListArray.Builder(StringType.Default).Build(),
                new List<IArrowArray?>(){ entriesDataArray }.BuildListArrayForType(entryType)
            };

            DenseUnionArray infoValue = new DenseUnionArray(infoUnionType, arrayLength, childrenArrays, typeBuilder.Build(), offsetBuilder.Build(), nullCount);

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                infoNameBuilder.Build(),
                infoValue
            };
            StandardSchemas.GetInfoSchema.Validate(dataArrays);

            return new SparkInfoArrowStream(StandardSchemas.GetInfoSchema, dataArrays);

        }

        public override IArrowArrayStream GetTableTypes()
        {
            StringArray.Builder tableTypesBuilder = new StringArray.Builder();
            tableTypesBuilder.AppendRange(new string[] { "BASE TABLE", "VIEW" });

            IArrowArray[] dataArrays = new IArrowArray[]
            {
                tableTypesBuilder.Build()
            };

            return new SparkInfoArrowStream(StandardSchemas.TableTypesSchema, dataArrays);
        }

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string? tableName)
        {
            TGetColumnsReq getColumnsReq = new TGetColumnsReq(this.sessionHandle);
            getColumnsReq.CatalogName = catalog;
            getColumnsReq.SchemaName = dbSchema;
            getColumnsReq.TableName = tableName;
            getColumnsReq.GetDirectResults = sparkGetDirectResults;

            var columnsResponse = this.Client.GetColumns(getColumnsReq).Result;
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
                // Note: the following two columns do not seem to be set correctly for DECIMAL types.
                //int? columnSize = columns[6].I32Val.Values.GetValue(i);
                //int? decimalDigits = columns[8].I32Val.Values.GetValue(i);
                bool nullable = columns[10].I32Val.Values.GetValue(i) == 1;
                IArrowType dataType = SparkConnection.GetArrowType((ColumnTypeId)columnType!.Value, typeName);
                fields[i] = new Field(columnName, dataType, nullable);
            }
            return new Schema(fields, null);
        }

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            Trace.TraceError($"getting objects with depth={depth.ToString()}, catalog = {catalogPattern}, dbschema = {dbSchemaPattern}, tablename = {tableNamePattern}");

            Dictionary<string, Dictionary<string, Dictionary<string, TableInfoPair>>> catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfoPair>>>();
            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
            {
                TGetCatalogsReq getCatalogsReq = new TGetCatalogsReq(this.sessionHandle);
                getCatalogsReq.GetDirectResults = sparkGetDirectResults;

                TGetCatalogsResp getCatalogsResp = this.Client.GetCatalogs(getCatalogsReq).Result;
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

                TGetSchemasResp getSchemasResp = this.Client.GetSchemas(getSchemasReq).Result;
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

                TGetTablesResp getTablesResp = this.Client.GetTables(getTablesReq).Result;
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
                    catalogMap.GetValueOrDefault(catalog)?.GetValueOrDefault(schemaDb)?.Add(tableName, tableInfo);
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

                var columnsResponse = this.Client.GetColumns(columnsReq).Result;
                if (columnsResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(columnsResponse.Status.ErrorMessage);
                }

                TRowSet resp = columnsResponse.DirectResults.ResultSet.Results;

                IReadOnlyList<string> catalogList = resp.Columns[0].StringVal.Values;
                IReadOnlyList<string> schemaList = resp.Columns[1].StringVal.Values;
                IReadOnlyList<string> tableList = resp.Columns[2].StringVal.Values;
                IReadOnlyList<string> columnList = resp.Columns[3].StringVal.Values;
                ReadOnlySpan<int> columnTypeList = resp.Columns[4].I32Val.Values.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    string tableName = tableList[i];
                    string column = columnList[i];
                    int colType = columnTypeList[i];
                    TableInfoPair? tableInfo = catalogMap.GetValueOrDefault(catalog)?.GetValueOrDefault(schemaDb)?.GetValueOrDefault(tableName);
                    tableInfo?.Columns.Add(column);
                    tableInfo?.ColType.Add(colType);
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

            Schema schema = StandardSchemas.GetObjectsSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    catalogNameBuilder.Build(),
                    catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema)),
                });

            return new SparkInfoArrowStream(schema, dataArrays);
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
                    return new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);
                case ColumnTypeId.BINARY_TYPE:
                    return BinaryType.Default;
                case ColumnTypeId.DATE_TYPE:
                    return Date32Type.Default;
                case ColumnTypeId.CHAR_TYPE:
                    return StringType.Default;
                case ColumnTypeId.DECIMAL_TYPE:
                    // Note: parsing the type name for SQL DECIMAL types as the precision and scale values
                    // are not returned in the Thrift call to GetColumns
                    return SqlDecimalTypeParser.ParseOrDefault(typeName, new Decimal128Type(DecimalPrecisionDefault, DecimalScaleDefault));
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

            IReadOnlyList<Field> schema = StandardSchemas.DbSchemaSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    dbSchemaNameBuilder.Build(),
                    dbSchemaTablesValues.BuildListArrayForType(new StructType(StandardSchemas.TableSchema)),
                });

            return new StructArray(
                new StructType(schema),
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


            IReadOnlyList<Field> schema = StandardSchemas.TableSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    tableNameBuilder.Build(),
                    tableTypeBuilder.Build(),
                    tableColumnsValues.BuildListArrayForType(new StructType(StandardSchemas.ColumnSchema)),
                    tableConstraintsValues.BuildListArrayForType( new StructType(StandardSchemas.ConstraintSchema))
                });

            return new StructArray(
                new StructType(schema),
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

            IReadOnlyList<Field> schema = StandardSchemas.ColumnSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
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
                });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private string PatternToRegEx(string? pattern)
        {
            if (pattern == null)
                return ".*";

            StringBuilder builder = new StringBuilder("(?i)^");
            string convertedPattern = pattern.Replace("_", ".").Replace("%", ".*");
            builder.Append(convertedPattern);
            builder.Append("$");

            return builder.ToString();
        }

        /// <summary>
        /// Provides a parser for SQL DECIMAL type definitions.
        /// </summary>
        private static class SqlDecimalTypeParser
        {
            // Pattern is based on this definition
            // https://docs.databricks.com/en/sql/language-manual/data-types/decimal-type.html#syntax
            // { DECIMAL | DEC | NUMERIC } [ (  p [ , s ] ) ]
            // p: Optional maximum precision (total number of digits) of the number between 1 and 38. The default is 10.
            // s: Optional scale of the number between 0 and p. The number of digits to the right of the decimal point. The default is 0.
            private static readonly Regex s_expression = new(
                @"^\s*(?<typeName>((DECIMAL)|(DEC)|(NUMERIC)))(\s*\(\s*((?<precision>\d{1,2})(\s*\,\s*(?<scale>\d{1,2}))?)\s*\))?\s*$",
                RegexOptions.IgnoreCase | RegexOptions.Compiled | RegexOptions.CultureInvariant);

            /// <summary>
            /// Parses the input string for a valid SQL DECIMAL type definition and returns a new <see cref="Decimal128Type"/> or returns the <c>defaultValue</c>, if invalid.
            /// </summary>
            /// <param name="input">The SQL type defintion string to parse.</param>
            /// <param name="defaultValue">If input string is an invalid SQL DECIMAL type definition, this value is returned instead.</param>
            /// <returns>If input string is a valid SQL DECIMAL type definition, it returns a new <see cref="Decimal128Type"/>; otherwise <c>defaultValue</c>.</returns>
            public static Decimal128Type ParseOrDefault(string input, Decimal128Type defaultValue)
            {
                return TryParse(input, out Decimal128Type? candidate) ? candidate! : defaultValue;
            }

            /// <summary>
            /// Tries to parse the input string for a valid SQL DECIMAL type definition.
            /// </summary>
            /// <param name="input">The SQL type defintion string to parse.</param>
            /// <param name="value">If successful, an new <see cref="Decimal128Type"/> with the precision and scale set; otherwise <c>null</c>.</param>
            /// <returns>True if it can successfully parse the type definition input string; otherwise false.</returns>
            private static bool TryParse(string input, out Decimal128Type? value)
            {
                // Ensure defaults are set, in case not provided in precision/scale clause.
                int precision = DecimalPrecisionDefault;
                int scale = DecimalScaleDefault;

                Match match = s_expression.Match(input);
                if (!match.Success)
                {
                    value = null;
                    return false;
                }

                GroupCollection groups = match.Groups;
                Group precisionGroup = groups["precision"];
                Group scaleGroup = groups["scale"];

                precision = precisionGroup.Success && int.TryParse(precisionGroup.Value, out int candidatePrecision) ? candidatePrecision : precision;
                scale = scaleGroup.Success && int.TryParse(scaleGroup.Value, out int candidateScale) ? candidateScale : scale;

                value = new Decimal128Type(precision, scale);
                return true;
            }
        }

        private string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? ProductVersionDefault;
        }
    }

    internal struct TableInfoPair
    {
        public string Type { get; set; }

        public List<string> Columns { get; set; }

        public List<int> ColType { get; set; }
    }

    internal class SparkInfoArrowStream : IArrowArrayStream
    {
        private Schema schema;
        private RecordBatch? batch;

        public SparkInfoArrowStream(Schema schema, IReadOnlyList<IArrowArray> data)
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
