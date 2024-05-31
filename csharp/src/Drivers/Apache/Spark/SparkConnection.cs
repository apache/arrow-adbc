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
using System.Linq;
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
        const string ColumnDef = "COLUMN_DEF";
        const string ColumnName = "COLUMN_NAME";
        const string DataType = "DATA_TYPE";
        const string IsAutoIncrement = "IS_AUTO_INCREMENT";
        const string IsNullable = "IS_NULLABLE";
        const string OrdinalPosition = "ORDINAL_POSITION";
        const string TableCat = "TABLE_CAT";
        const string TableCatalog = "TABLE_CATALOG";
        const string TableName = "TABLE_NAME";
        const string TableSchem = "TABLE_SCHEM";
        const string TableType = "TABLE_TYPE";
        const string TypeName = "TYPE_NAME";
        const string Nullable = "NULLABLE";
        private readonly Lazy<string> _productVersion;

        internal static TSparkGetDirectResults sparkGetDirectResults = new TSparkGetDirectResults(1000);

        internal static readonly Dictionary<string, string> timestampConfig = new Dictionary<string, string>
        {
            { "spark.thriftserver.arrowBasedRowSet.timestampAsString", "false" }
        };

        /// <summary>
        /// The Spark data type definitions based on the <see href="https://docs.oracle.com/en%2Fjava%2Fjavase%2F21%2Fdocs%2Fapi%2F%2F/java.sql/java/sql/Types.html">JDBC Types</see> constants.
        /// </summary>
        /// <remarks>
        /// This enumeration can be used to determine the Spark-specific data types that are contained in fields <c>xdbc_data_type</c> and <c>xdbc_sql_data_type</c>
        /// in the column metadata <see cref="StandardSchemas.ColumnSchema"/>. This column metadata is returned as a result of a call to
        /// <see cref="AdbcConnection.GetObjects(GetObjectsDepth, string?, string?, string?, IReadOnlyList{string}?, string?)"/>
        /// when <c>depth</c> is set to <see cref="AdbcConnection.GetObjectsDepth.All"/>.
        /// </remarks>
        internal enum ColumnTypeId
        {
            // NOTE: There is a partial copy of this enumeration in test/Drivers/Apache/Spark/DriverTests.cs
            // Please keep up-to-date.
            // Copied from https://docs.oracle.com/en%2Fjava%2Fjavase%2F21%2Fdocs%2Fapi%2F%2F/constant-values.html#java.sql.Types.ARRAY

            /// <summary>
            /// Identifies the generic SQL type ARRAY
            /// </summary>
            ARRAY = 2003,
            /// <summary>
            /// Identifies the generic SQL type BIGINT
            /// </summary>
            BIGINT = -5,
            /// <summary>
            /// Identifies the generic SQL type BINARY
            /// </summary>
            BINARY = -2,
            /// <summary>
            /// Identifies the generic SQL type BOOLEAN
            /// </summary>
            BOOLEAN = 16,
            /// <summary>
            /// Identifies the generic SQL type CHAR
            /// </summary>
            CHAR = 1,
            /// <summary>
            /// Identifies the generic SQL type DATE
            /// </summary>
            DATE = 91,
            /// <summary>
            /// Identifies the generic SQL type DECIMAL
            /// </summary>
            DECIMAL = 3,
            /// <summary>
            /// Identifies the generic SQL type DOUBLE
            /// </summary>
            DOUBLE = 8,
            /// <summary>
            /// Identifies the generic SQL type FLOAT
            /// </summary>
            FLOAT = 6,
            /// <summary>
            /// Identifies the generic SQL type INTEGER
            /// </summary>
            INTEGER = 4,
            /// <summary>
            /// Identifies the generic SQL type JAVA_OBJECT (MAP)
            /// </summary>
            JAVA_OBJECT = 2000,
            /// <summary>
            /// identifies the generic SQL type LONGNVARCHAR
            /// </summary>
            LONGNVARCHAR = -16,
            /// <summary>
            /// identifies the generic SQL type LONGVARBINARY
            /// </summary>
            LONGVARBINARY = -4,
            /// <summary>
            /// identifies the generic SQL type LONGVARCHAR
            /// </summary>
            LONGVARCHAR = -1,
            /// <summary>
            /// identifies the generic SQL type NCHAR
            /// </summary>
            NCHAR = -15,
            /// <summary>
            /// identifies the generic SQL value NULL
            /// </summary>
            NULL = 0,
            /// <summary>
            /// identifies the generic SQL type NUMERIC
            /// </summary>
            NUMERIC = 2,
            /// <summary>
            /// identifies the generic SQL type NVARCHAR
            /// </summary>
            NVARCHAR = -9,
            /// <summary>
            /// identifies the generic SQL type REAL
            /// </summary>
            REAL = 7,
            /// <summary>
            /// Identifies the generic SQL type SMALLINT
            /// </summary>
            SMALLINT = 5,
            /// <summary>
            /// Identifies the generic SQL type STRUCT
            /// </summary>
            STRUCT = 2002,
            /// <summary>
            /// Identifies the generic SQL type TIMESTAMP
            /// </summary>
            TIMESTAMP = 93,
            /// <summary>
            /// Identifies the generic SQL type TINYINT
            /// </summary>
            TINYINT = -6,
            /// <summary>
            /// Identifies the generic SQL type VARBINARY
            /// </summary>
            VARBINARY = -3,
            /// <summary>
            /// Identifies the generic SQL type VARCHAR
            /// </summary>
            VARCHAR = 12,
            // ======================
            // Unused/unsupported
            // ======================
            /// <summary>
            /// Identifies the generic SQL type BIT
            /// </summary>
            BIT = -7,
            /// <summary>
            /// Identifies the generic SQL type BLOB
            /// </summary>
            BLOB = 2004,
            /// <summary>
            /// Identifies the generic SQL type CLOB
            /// </summary>
            CLOB = 2005,
            /// <summary>
            /// Identifies the generic SQL type DATALINK
            /// </summary>
            DATALINK = 70,
            /// <summary>
            /// Identifies the generic SQL type DISTINCT
            /// </summary>
            DISTINCT = 2001,
            /// <summary>
            /// identifies the generic SQL type NCLOB
            /// </summary>
            NCLOB = 2011,
            /// <summary>
            /// Indicates that the SQL type is database-specific and gets mapped to a Java object
            /// </summary>
            OTHER = 1111,
            /// <summary>
            /// Identifies the generic SQL type REF CURSOR
            /// </summary>
            REF_CURSOR = 2012,
            /// <summary>
            /// Identifies the generic SQL type REF
            /// </summary>
            REF = 2006,
            /// <summary>
            /// Identifies the generic SQL type ROWID
            /// </summary>
            ROWID = -8,
            /// <summary>
            /// Identifies the generic SQL type XML
            /// </summary>
            SQLXML = 2009,
            /// <summary>
            /// Identifies the generic SQL type TIME
            /// </summary>
            TIME = 92,
            /// <summary>
            /// Identifies the generic SQL type TIME WITH TIMEZONE
            /// </summary>
            TIME_WITH_TIMEZONE = 2013,
            /// <summary>
            /// Identifies the generic SQL type TIMESTAMP WITH TIMEZONE
            /// </summary>
            TIMESTAMP_WITH_TIMEZONE = 2014,
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
            TGetTableTypesReq req = new()
            {
                SessionHandle = this.sessionHandle ?? throw new InvalidOperationException("session not created"),
                GetDirectResults = sparkGetDirectResults
            };
            TGetTableTypesResp resp = this.Client.GetTableTypes(req).Result;
            if (resp.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(resp.Status.ErrorMessage)
                    .SetNativeError(resp.Status.ErrorCode)
                    .SetSqlState(resp.Status.SqlState);
            }

            List<TColumn> columns = resp.DirectResults.ResultSet.Results.Columns;
            StringArray tableTypes = columns[0].StringVal.Values;

            StringArray.Builder tableTypesBuilder = new StringArray.Builder();
            tableTypesBuilder.AppendRange(tableTypes);

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
                IArrowType dataType = SparkConnection.GetArrowType(columnType!.Value, typeName);
                fields[i] = new Field(columnName, dataType, nullable);
            }
            return new Schema(fields, null);
        }

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            Trace.TraceError($"getting objects with depth={depth.ToString()}, catalog = {catalogPattern}, dbschema = {dbSchemaPattern}, tablename = {tableNamePattern}");

            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>>();
            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
            {
                TGetCatalogsReq getCatalogsReq = new TGetCatalogsReq(this.sessionHandle);
                getCatalogsReq.GetDirectResults = sparkGetDirectResults;

                TGetCatalogsResp getCatalogsResp = this.Client.GetCatalogs(getCatalogsReq).Result;
                if (getCatalogsResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(getCatalogsResp.Status.ErrorMessage);
                }
                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(getCatalogsResp.DirectResults.ResultSetMetadata.Schema.Columns);

                string catalogRegexp = PatternToRegEx(catalogPattern);
                TRowSet resp = getCatalogsResp.DirectResults.ResultSet.Results;
                IReadOnlyList<string> list = resp.Columns[columnMap[TableCat]].StringVal.Values;
                for (int i = 0; i < list.Count; i++)
                {
                    string col = list[i];
                    string catalog = col;

                    if (Regex.IsMatch(catalog, catalogRegexp, RegexOptions.IgnoreCase))
                    {
                        catalogMap.Add(catalog, new Dictionary<string, Dictionary<string, TableInfo>>());
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
                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(getSchemasResp.DirectResults.ResultSetMetadata.Schema.Columns);
                TRowSet resp = getSchemasResp.DirectResults.ResultSet.Results;

                IReadOnlyList<string> catalogList = resp.Columns[columnMap[TableCatalog]].StringVal.Values;
                IReadOnlyList<string> schemaList = resp.Columns[columnMap[TableSchem]].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    // It seems Spark sometimes returns empty string for catalog on some schema (temporary tables).
                    catalogMap.GetValueOrDefault(catalog)?.Add(schemaDb, new Dictionary<string, TableInfo>());
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

                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(getTablesResp.DirectResults.ResultSetMetadata.Schema.Columns);
                TRowSet resp = getTablesResp.DirectResults.ResultSet.Results;

                IReadOnlyList<string> catalogList = resp.Columns[columnMap[TableCat]].StringVal.Values;
                IReadOnlyList<string> schemaList = resp.Columns[columnMap[TableSchem]].StringVal.Values;
                IReadOnlyList<string> tableList = resp.Columns[columnMap[TableName]].StringVal.Values;
                IReadOnlyList<string> tableTypeList = resp.Columns[columnMap[TableType]].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    string tableName = tableList[i];
                    string tableType = tableTypeList[i];
                    TableInfo tableInfo = new(tableType);
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

                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(columnsResponse.DirectResults.ResultSetMetadata.Schema.Columns);
                TRowSet resp = columnsResponse.DirectResults.ResultSet.Results;

                IReadOnlyList<string> catalogList = resp.Columns[columnMap[TableCat]].StringVal.Values;
                IReadOnlyList<string> schemaList = resp.Columns[columnMap[TableSchem]].StringVal.Values;
                IReadOnlyList<string> tableList = resp.Columns[columnMap[TableName]].StringVal.Values;
                IReadOnlyList<string> columnNameList = resp.Columns[columnMap[ColumnName]].StringVal.Values;
                ReadOnlySpan<int> columnTypeList = resp.Columns[columnMap[DataType]].I32Val.Values.Values;
                IReadOnlyList<string> typeNameList = resp.Columns[columnMap[TypeName]].StringVal.Values;
                ReadOnlySpan<int> nullableList = resp.Columns[columnMap[Nullable]].I32Val.Values.Values;
                IReadOnlyList<string> columnDefaultList = resp.Columns[columnMap[ColumnDef]].StringVal.Values;
                ReadOnlySpan<int> ordinalPosList = resp.Columns[columnMap[OrdinalPosition]].I32Val.Values.Values;
                IReadOnlyList<string> isNullableList = resp.Columns[columnMap[IsNullable]].StringVal.Values;
                IReadOnlyList<string> isAutoIncrementList = resp.Columns[columnMap[IsAutoIncrement]].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    string tableName = tableList[i];
                    string columnName = columnNameList[i];
                    short colType = (short)columnTypeList[i];
                    string typeName = typeNameList[i];
                    short nullable = (short)nullableList[i];
                    string? isAutoIncrementString = isAutoIncrementList[i];
                    bool isAutoIncrement = (!string.IsNullOrEmpty(isAutoIncrementString) && (isAutoIncrementString.Equals("YES", StringComparison.InvariantCultureIgnoreCase) || isAutoIncrementString.Equals("TRUE", StringComparison.InvariantCultureIgnoreCase)));
                    string isNullable = isNullableList[i] ?? "YES";
                    string columnDefault = columnDefaultList[i] ?? "";
                    // Spark/Databricks reports ordinal index zero-indexed, instead of one-indexed
                    int ordinalPos = ordinalPosList[i] + 1;
                    TableInfo? tableInfo = catalogMap.GetValueOrDefault(catalog)?.GetValueOrDefault(schemaDb)?.GetValueOrDefault(tableName);
                    tableInfo?.ColumnName.Add(columnName);
                    tableInfo?.ColType.Add(colType);
                    tableInfo?.Nullable.Add(nullable);
                    tableInfo?.TypeName.Add(typeName);
                    tableInfo?.IsAutoIncrement.Add(isAutoIncrement);
                    tableInfo?.IsNullable.Add(isNullable);
                    tableInfo?.ColumnDefault.Add(columnDefault);
                    tableInfo?.OrdinalPosition.Add(ordinalPos);
                    SetPrecisionAndScale(colType, typeName, tableInfo);
                }
            }

            StringArray.Builder catalogNameBuilder = new StringArray.Builder();
            List<IArrowArray?> catalogDbSchemasValues = new List<IArrowArray?>();

            foreach (KeyValuePair<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogEntry in catalogMap)
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

        private static IReadOnlyDictionary<string, int> GetColumnIndexMap(List<TColumnDesc> columns) => columns
            .Select(t => new { Index = t.Position - 1, t.ColumnName })
            .ToDictionary(t => t.ColumnName, t => t.Index);

        private static void SetPrecisionAndScale(short colType, string typeName, TableInfo? tableInfo)
        {
            switch (colType)
            {
                case (short)ColumnTypeId.DECIMAL:
                case (short)ColumnTypeId.NUMERIC:
                    {
                        Decimal128Type decimalType = SqlDecimalTypeParser.ParseOrDefault(typeName, new Decimal128Type(DecimalPrecisionDefault, DecimalScaleDefault));
                        tableInfo?.Precision.Add(decimalType.Precision);
                        tableInfo?.Scale.Add((short)decimalType.Scale);
                        break;
                    }

                default:
                    tableInfo?.Precision.Add(null);
                    tableInfo?.Scale.Add(null);
                    break;
            }
        }

        private static IArrowType GetArrowType(int columnTypeId, string typeName)
        {
            switch (columnTypeId)
            {
                case (int)ColumnTypeId.BOOLEAN:
                    return BooleanType.Default;
                case (int)ColumnTypeId.TINYINT:
                    return Int8Type.Default;
                case (int)ColumnTypeId.SMALLINT:
                    return Int16Type.Default;
                case (int)ColumnTypeId.INTEGER:
                    return Int32Type.Default;
                case (int)ColumnTypeId.BIGINT:
                    return Int64Type.Default;
                case (int)ColumnTypeId.FLOAT:
                case (int)ColumnTypeId.REAL:
                    return FloatType.Default;
                case (int)ColumnTypeId.DOUBLE:
                    return DoubleType.Default;
                case (int)ColumnTypeId.VARCHAR:
                case (int)ColumnTypeId.NVARCHAR:
                case (int)ColumnTypeId.LONGVARCHAR:
                case (int)ColumnTypeId.LONGNVARCHAR:
                    return StringType.Default;
                case (int)ColumnTypeId.TIMESTAMP:
                    return new TimestampType(TimeUnit.Microsecond, timezone: (string?)null);
                case (int)ColumnTypeId.BINARY:
                case (int)ColumnTypeId.VARBINARY:
                case (int)ColumnTypeId.LONGVARBINARY:
                    return BinaryType.Default;
                case (int)ColumnTypeId.DATE:
                    return Date32Type.Default;
                case (int)ColumnTypeId.CHAR:
                case (int)ColumnTypeId.NCHAR:
                    return StringType.Default;
                case (int)ColumnTypeId.DECIMAL:
                case (int)ColumnTypeId.NUMERIC:
                    // Note: parsing the type name for SQL DECIMAL types as the precision and scale values
                    // are not returned in the Thrift call to GetColumns
                    return SqlDecimalTypeParser.ParseOrDefault(typeName, new Decimal128Type(DecimalPrecisionDefault, DecimalScaleDefault));
                case (int)ColumnTypeId.NULL:
                    return NullType.Default;
                case (int)ColumnTypeId.ARRAY:
                case (int)ColumnTypeId.JAVA_OBJECT:
                case (int)ColumnTypeId.STRUCT:
                    return StringType.Default;
                default:
                    throw new NotImplementedException($"Column type id: {columnTypeId} is not supported.");
            }
        }

        private StructArray GetDbSchemas(
            GetObjectsDepth depth,
            Dictionary<string, Dictionary<string, TableInfo>> schemaMap)
        {
            StringArray.Builder dbSchemaNameBuilder = new StringArray.Builder();
            List<IArrowArray?> dbSchemaTablesValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            foreach (KeyValuePair<string, Dictionary<string, TableInfo>> schemaEntry in schemaMap)
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
            Dictionary<string, TableInfo> tableMap)
        {
            StringArray.Builder tableNameBuilder = new StringArray.Builder();
            StringArray.Builder tableTypeBuilder = new StringArray.Builder();
            List<IArrowArray?> tableColumnsValues = new List<IArrowArray?>();
            List<IArrowArray?> tableConstraintsValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            foreach (KeyValuePair<string, TableInfo> tableEntry in tableMap)
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
                    tableColumnsValues.Add(GetColumnSchema(tableEntry.Value));
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

        private StructArray GetColumnSchema(TableInfo tableInfo)
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


            for (int i = 0; i < tableInfo.ColumnName.Count; i++)
            {
                columnNameBuilder.Append(tableInfo.ColumnName[i]);
                ordinalPositionBuilder.Append(tableInfo.OrdinalPosition[i]);
                remarksBuilder.Append("");
                xdbcColumnSizeBuilder.Append(tableInfo.Precision[i]);
                xdbcDecimalDigitsBuilder.Append(tableInfo.Scale[i]);
                xdbcDataTypeBuilder.Append(tableInfo.ColType[i]);
                xdbcTypeNameBuilder.Append(tableInfo.TypeName[i]);
                xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.Append(tableInfo.Nullable[i]);
                xdbcColumnDefBuilder.Append(tableInfo.ColumnDefault[i]);
                xdbcSqlDataTypeBuilder.Append(tableInfo.ColType[i]);
                xdbcDatetimeSubBuilder.AppendNull();
                xdbcCharOctetLengthBuilder.AppendNull();
                xdbcIsNullableBuilder.Append(tableInfo.IsNullable[i]);
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.Append(tableInfo.IsAutoIncrement[i]);
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

    internal struct TableInfo(string type)
    {
        public string Type { get; } = type;

        public List<string> ColumnName { get; } = new();

        public List<short> ColType { get; } = new();

        public List<string> TypeName { get; } = new();

        public List<short> Nullable { get; } = new();

        public List<int?> Precision { get; } = new();

        public List<short?> Scale { get; } = new();

        public List<int> OrdinalPosition { get; } = new();

        public List<string> ColumnDefault { get; } = new();

        public List<string> IsNullable { get; } = new();

        public List<bool> IsAutoIncrement { get; } = new();
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
