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
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Thrift;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal abstract class HiveServer2Connection : AdbcConnection
    {
        internal const long BatchSizeDefault = 50000;
        internal const int PollTimeMillisecondsDefault = 500;
        private const int ConnectTimeoutMillisecondsDefault = 30000;
        private TTransport? _transport;
        private TCLIService.Client? _client;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;

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

        /// <summary>
        /// The data type definitions based on the <see href="https://docs.oracle.com/en%2Fjava%2Fjavase%2F21%2Fdocs%2Fapi%2F%2F/java.sql/java/sql/Types.html">JDBC Types</see> constants.
        /// </summary>
        /// <remarks>
        /// This enumeration can be used to determine the drivers specific data types that are contained in fields <c>xdbc_data_type</c> and <c>xdbc_sql_data_type</c>
        /// in the column metadata <see cref="StandardSchemas.ColumnSchema"/>. This column metadata is returned as a result of a call to
        /// <see cref="AdbcConnection.GetObjects(GetObjectsDepth, string?, string?, string?, IReadOnlyList{string}?, string?)"/>
        /// when <c>depth</c> is set to <see cref="AdbcConnection.GetObjectsDepth.All"/>.
        /// </remarks>
        internal enum ColumnTypeId
        {
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
            /// identifies the generic SQL type NULL
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

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            Properties = properties;
            // Note: "LazyThreadSafetyMode.PublicationOnly" is thread-safe initialization where
            // the first successful thread sets the value. If an exception is thrown, initialization
            // will retry until it successfully returns a value without an exception.
            // https://learn.microsoft.com/en-us/dotnet/framework/performance/lazy-initialization#exceptions-in-lazy-objects
            _vendorVersion = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_VER), LazyThreadSafetyMode.PublicationOnly);
            _vendorName = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_NAME), LazyThreadSafetyMode.PublicationOnly);

            if (properties.TryGetValue(ApacheParameters.QueryTimeoutSeconds, out string? queryTimeoutSecondsSettingValue))
            {
                if (ApacheUtility.QueryTimeoutIsValid(ApacheParameters.QueryTimeoutSeconds, queryTimeoutSecondsSettingValue, out int queryTimeoutSeconds))
                {
                    QueryTimeoutSeconds = queryTimeoutSeconds;
                }
            }
        }

        internal TCLIService.Client Client
        {
            get { return _client ?? throw new InvalidOperationException("connection not open"); }
        }

        internal string VendorVersion => _vendorVersion.Value;

        internal string VendorName => _vendorName.Value;

        protected internal int QueryTimeoutSeconds { get; set; } = ApacheUtility.QueryTimeoutSecondsDefault;

        internal IReadOnlyDictionary<string, string> Properties { get; }

        internal async Task OpenAsync()
        {
            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(ConnectTimeoutMilliseconds, ApacheUtility.TimeUnit.Milliseconds);
            try
            {
                TTransport transport = CreateTransport();
                TProtocol protocol = await CreateProtocolAsync(transport, cancellationToken);
                _transport = protocol.Transport;
                _client = new TCLIService.Client(protocol);
                TOpenSessionReq request = CreateSessionRequest();

                TOpenSessionResp? session = await Client.OpenSession(request, cancellationToken);

                // Explicitly check the session status
                if (session == null)
                {
                    throw new HiveServer2Exception("Unable to open session. Unknown error.");
                }
                else if (session.Status.StatusCode != TStatusCode.SUCCESS_STATUS)
                {
                    throw new HiveServer2Exception(session.Status.ErrorMessage)
                        .SetNativeError(session.Status.ErrorCode)
                        .SetSqlState(session.Status.SqlState);
                }

                SessionHandle = session.SessionHandle;
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The operation timed out while attempting to open a session. Please try increasing connect timeout.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                // Handle other exceptions if necessary
                throw new HiveServer2Exception($"An unexpected error occurred while opening the session. '{ex.Message}'", ex);
            }
        }

        internal TSessionHandle? SessionHandle { get; private set; }

        protected internal DataTypeConversion DataTypeConversion { get; set; } = DataTypeConversion.None;

        protected internal HiveServer2TlsOption TlsOptions { get; set; } = HiveServer2TlsOption.Empty;

        protected internal int ConnectTimeoutMilliseconds { get; set; } = ConnectTimeoutMillisecondsDefault;

        protected abstract TTransport CreateTransport();

        protected abstract Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default);

        protected abstract TOpenSessionReq CreateSessionRequest();

        internal abstract SchemaParser SchemaParser { get; }

        internal abstract IArrowArrayStream NewReader<T>(T statement, Schema schema) where T : HiveServer2Statement;

        internal static async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds, CancellationToken cancellationToken = default)
        {
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { await Task.Delay(pollTimeMilliseconds, cancellationToken); }
                TGetOperationStatusReq request = new(operationHandle);
                statusResponse = await client.GetOperationStatus(request, cancellationToken);
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
        }

        private string GetInfoTypeStringValue(TGetInfoType infoType)
        {
            TGetInfoReq req = new()
            {
                SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
                InfoType = infoType,
            };

            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                TGetInfoResp getInfoResp = Client.GetInfo(req, cancellationToken).Result;
                if (getInfoResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new HiveServer2Exception(getInfoResp.Status.ErrorMessage)
                        .SetNativeError(getInfoResp.Status.ErrorCode)
                        .SetSqlState(getInfoResp.Status.SqlState);
                }

                return getInfoResp.InfoValue.StringValue;
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ex.Message}'", ex);
            }
        }

        public override void Dispose()
        {
            if (_client != null)
            {
                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                TCloseSessionReq r6 = new(SessionHandle);
                _client.CloseSession(r6, cancellationToken).Wait();
                _transport?.Close();
                _client.Dispose();
                _transport = null;
                _client = null;
            }
        }

        internal static async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataReq request = new(operationHandle);
            TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
            return response;
        }

        protected abstract string GetProductVersionDefault();

        protected internal string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? GetProductVersionDefault();
        }

        protected static Uri GetBaseAddress(string? uri, string? hostName, string? path, string? port)
        {
            // Uri property takes precedent.
            if (!string.IsNullOrWhiteSpace(uri))
            {
                var uriValue = new Uri(uri);
                if (uriValue.Scheme != Uri.UriSchemeHttp && uriValue.Scheme != Uri.UriSchemeHttps)
                    throw new ArgumentOutOfRangeException(
                        AdbcOptions.Uri,
                        uri,
                        $"Unsupported scheme '{uriValue.Scheme}'");
                return uriValue;
            }

            bool isPortSet = !string.IsNullOrEmpty(port);
            bool isValidPortNumber = int.TryParse(port, out int portNumber) && portNumber > 0;
            bool isDefaultHttpsPort = !isPortSet || (isValidPortNumber && portNumber == 443);
            string uriScheme = isDefaultHttpsPort ? Uri.UriSchemeHttps : Uri.UriSchemeHttp;
            int uriPort;
            if (!isPortSet)
                uriPort = -1;
            else if (isValidPortNumber)
                uriPort = portNumber;
            else
                throw new ArgumentOutOfRangeException(nameof(port), portNumber, $"Port number is not in a valid range.");

            Uri baseAddress = new UriBuilder(uriScheme, hostName, uriPort, path).Uri;
            return baseAddress;
        }

        protected abstract Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response, CancellationToken cancellationToken = default);
        protected abstract Task<TRowSet> GetRowSetAsync(TGetColumnsResp response, CancellationToken cancellationToken = default);
        protected abstract Task<TRowSet> GetRowSetAsync(TGetTablesResp response, CancellationToken cancellationToken = default);
        protected abstract Task<TRowSet> GetRowSetAsync(TGetCatalogsResp getCatalogsResp, CancellationToken cancellationToken = default);
        protected abstract Task<TRowSet> GetRowSetAsync(TGetSchemasResp getSchemasResp, CancellationToken cancellationToken = default);
        protected abstract Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response, CancellationToken cancellationToken = default);
        protected abstract Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default);
        protected abstract Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response, CancellationToken cancellationToken = default);
        protected abstract Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response, CancellationToken cancellationToken = default);

        protected abstract bool AreResultsAvailableDirectly();

        protected abstract TSparkGetDirectResults GetDirectResults();

        protected abstract IReadOnlyDictionary<string, int> GetColumnIndexMap(List<TColumnDesc> columns);

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>>();
            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
                {
                    TGetCatalogsReq getCatalogsReq = new TGetCatalogsReq(SessionHandle);
                    if (AreResultsAvailableDirectly())
                    {
                        getCatalogsReq.GetDirectResults = GetDirectResults();
                    }

                    TGetCatalogsResp getCatalogsResp = Client.GetCatalogs(getCatalogsReq, cancellationToken).Result;

                    if (getCatalogsResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                    {
                        throw new Exception(getCatalogsResp.Status.ErrorMessage);
                    }
                    var catalogsMetadata = GetResultSetMetadataAsync(getCatalogsResp, cancellationToken).Result;
                    IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(catalogsMetadata.Schema.Columns);

                    string catalogRegexp = PatternToRegEx(catalogPattern);
                    TRowSet rowSet = GetRowSetAsync(getCatalogsResp, cancellationToken).Result;
                    IReadOnlyList<string> list = rowSet.Columns[columnMap[TableCat]].StringVal.Values;
                    for (int i = 0; i < list.Count; i++)
                    {
                        string col = list[i];
                        string catalog = col;

                        if (Regex.IsMatch(catalog, catalogRegexp, RegexOptions.IgnoreCase))
                        {
                            catalogMap.Add(catalog, new Dictionary<string, Dictionary<string, TableInfo>>());
                        }
                    }
                    // Handle the case where server does not support 'catalog' in the namespace.
                    if (list.Count == 0 && string.IsNullOrEmpty(catalogPattern))
                    {
                        catalogMap.Add(string.Empty, []);
                    }
                }

                if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.DbSchemas)
                {
                    TGetSchemasReq getSchemasReq = new TGetSchemasReq(SessionHandle);
                    getSchemasReq.CatalogName = catalogPattern;
                    getSchemasReq.SchemaName = dbSchemaPattern;
                    if (AreResultsAvailableDirectly())
                    {
                        getSchemasReq.GetDirectResults = GetDirectResults();
                    }

                    TGetSchemasResp getSchemasResp = Client.GetSchemas(getSchemasReq, cancellationToken).Result;
                    if (getSchemasResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                    {
                        throw new Exception(getSchemasResp.Status.ErrorMessage);
                    }

                    TGetResultSetMetadataResp schemaMetadata = GetResultSetMetadataAsync(getSchemasResp, cancellationToken).Result;
                    IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(schemaMetadata.Schema.Columns);
                    TRowSet rowSet = GetRowSetAsync(getSchemasResp, cancellationToken).Result;

                    IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCatalog]].StringVal.Values;
                    IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;

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
                    TGetTablesReq getTablesReq = new TGetTablesReq(SessionHandle);
                    getTablesReq.CatalogName = catalogPattern;
                    getTablesReq.SchemaName = dbSchemaPattern;
                    getTablesReq.TableName = tableNamePattern;
                    if (AreResultsAvailableDirectly())
                    {
                        getTablesReq.GetDirectResults = GetDirectResults();
                    }

                    TGetTablesResp getTablesResp = Client.GetTables(getTablesReq, cancellationToken).Result;
                    if (getTablesResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                    {
                        throw new Exception(getTablesResp.Status.ErrorMessage);
                    }

                    TGetResultSetMetadataResp tableMetadata = GetResultSetMetadataAsync(getTablesResp, cancellationToken).Result;
                    IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(tableMetadata.Schema.Columns);
                    TRowSet rowSet = GetRowSetAsync(getTablesResp, cancellationToken).Result;

                    IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCat]].StringVal.Values;
                    IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;
                    IReadOnlyList<string> tableList = rowSet.Columns[columnMap[TableName]].StringVal.Values;
                    IReadOnlyList<string> tableTypeList = rowSet.Columns[columnMap[TableType]].StringVal.Values;

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
                    TGetColumnsReq columnsReq = new TGetColumnsReq(SessionHandle);
                    columnsReq.CatalogName = catalogPattern;
                    columnsReq.SchemaName = dbSchemaPattern;
                    columnsReq.TableName = tableNamePattern;
                    if (AreResultsAvailableDirectly())
                    {
                        columnsReq.GetDirectResults = GetDirectResults();
                    }

                    if (!string.IsNullOrEmpty(columnNamePattern))
                        columnsReq.ColumnName = columnNamePattern;

                    var columnsResponse = Client.GetColumns(columnsReq, cancellationToken).Result;
                    if (columnsResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
                    {
                        throw new Exception(columnsResponse.Status.ErrorMessage);
                    }

                    TGetResultSetMetadataResp columnsMetadata = GetResultSetMetadataAsync(columnsResponse, cancellationToken).Result;
                    IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(columnsMetadata.Schema.Columns);
                    TRowSet rowSet = GetRowSetAsync(columnsResponse, cancellationToken).Result;

                    IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCat]].StringVal.Values;
                    IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;
                    IReadOnlyList<string> tableList = rowSet.Columns[columnMap[TableName]].StringVal.Values;
                    IReadOnlyList<string> columnNameList = rowSet.Columns[columnMap[ColumnName]].StringVal.Values;
                    ReadOnlySpan<int> columnTypeList = rowSet.Columns[columnMap[DataType]].I32Val.Values.Values;
                    IReadOnlyList<string> typeNameList = rowSet.Columns[columnMap[TypeName]].StringVal.Values;
                    ReadOnlySpan<int> nullableList = rowSet.Columns[columnMap[Nullable]].I32Val.Values.Values;
                    IReadOnlyList<string> columnDefaultList = rowSet.Columns[columnMap[ColumnDef]].StringVal.Values;
                    ReadOnlySpan<int> ordinalPosList = rowSet.Columns[columnMap[OrdinalPosition]].I32Val.Values.Values;
                    IReadOnlyList<string> isNullableList = rowSet.Columns[columnMap[IsNullable]].StringVal.Values;
                    IReadOnlyList<string> isAutoIncrementList = rowSet.Columns[columnMap[IsAutoIncrement]].StringVal.Values;

                    for (int i = 0; i < catalogList.Count; i++)
                    {
                        // For systems that don't support 'catalog' in the namespace
                        string catalog = catalogList[i] ?? string.Empty;
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
                        tableInfo?.IsAutoIncrement.Add(isAutoIncrement);
                        tableInfo?.IsNullable.Add(isNullable);
                        tableInfo?.ColumnDefault.Add(columnDefault);
                        tableInfo?.OrdinalPosition.Add(ordinalPos);
                        SetPrecisionScaleAndTypeName(colType, typeName, tableInfo);
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

                return new HiveInfoArrowStream(schema, dataArrays);
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ex.Message}'", ex);
            }
        }

        private static string PatternToRegEx(string? pattern)
        {
            if (pattern == null)
                return ".*";

            StringBuilder builder = new StringBuilder("(?i)^");
            string convertedPattern = pattern.Replace("_", ".").Replace("%", ".*");
            builder.Append(convertedPattern);
            builder.Append('$');

            return builder.ToString();
        }

        private static StructArray GetDbSchemas(
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

        private static StructArray GetTableSchemas(
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

        private static StructArray GetColumnSchema(TableInfo tableInfo)
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
                // Use the "remarks" field to store the original type name value
                remarksBuilder.Append(tableInfo.TypeName[i]);
                xdbcColumnSizeBuilder.Append(tableInfo.Precision[i]);
                xdbcDecimalDigitsBuilder.Append(tableInfo.Scale[i]);
                xdbcDataTypeBuilder.Append(tableInfo.ColType[i]);
                // Just the base type name without precision or scale clause
                xdbcTypeNameBuilder.Append(tableInfo.BaseTypeName[i]);
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

        private static void SetPrecisionScaleAndTypeName(short colType, string typeName, TableInfo? tableInfo)
        {
            // Keep the original type name
            tableInfo?.TypeName.Add(typeName);
            switch (colType)
            {
                case (short)ColumnTypeId.DECIMAL:
                case (short)ColumnTypeId.NUMERIC:
                    {
                        SqlDecimalParserResult result = SqlTypeNameParser<SqlDecimalParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(result.Precision);
                        tableInfo?.Scale.Add((short)result.Scale);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                case (short)ColumnTypeId.CHAR:
                case (short)ColumnTypeId.NCHAR:
                case (short)ColumnTypeId.VARCHAR:
                case (short)ColumnTypeId.LONGVARCHAR:
                case (short)ColumnTypeId.LONGNVARCHAR:
                case (short)ColumnTypeId.NVARCHAR:
                    {
                        SqlCharVarcharParserResult result = SqlTypeNameParser<SqlCharVarcharParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(result.ColumnSize);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                default:
                    {
                        SqlTypeNameParserResult result = SqlTypeNameParser<SqlTypeNameParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(null);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }
            }
        }

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string? tableName)
        {
            TGetColumnsReq getColumnsReq = new TGetColumnsReq(SessionHandle);
            getColumnsReq.CatalogName = catalog;
            getColumnsReq.SchemaName = dbSchema;
            getColumnsReq.TableName = tableName;
            if (AreResultsAvailableDirectly())
            {
                getColumnsReq.GetDirectResults = GetDirectResults();
            }

            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                var columnsResponse = Client.GetColumns(getColumnsReq, cancellationToken).Result;
                if (columnsResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(columnsResponse.Status.ErrorMessage);
                }

                TRowSet rowSet = GetRowSetAsync(columnsResponse, cancellationToken).Result;
                List<TColumn> columns = rowSet.Columns;
                int rowCount = rowSet.Columns[3].StringVal.Values.Length;

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
                    IArrowType dataType = HiveServer2Connection.GetArrowType(columnType!.Value, typeName);
                    fields[i] = new Field(columnName, dataType, nullable);
                }
                return new Schema(fields, null);
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ex.Message}'", ex);
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
                    return SqlTypeNameParser<SqlDecimalParserResult>
                        .Parse(typeName, columnTypeId)
                        .Decimal128Type;
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

        public override IArrowArrayStream GetTableTypes()
        {
            TGetTableTypesReq req = new()
            {
                SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
            };

            if (AreResultsAvailableDirectly())
            {
                req.GetDirectResults = GetDirectResults();
            }

            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                TGetTableTypesResp resp = Client.GetTableTypes(req, cancellationToken).Result;

                if (resp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new HiveServer2Exception(resp.Status.ErrorMessage)
                        .SetNativeError(resp.Status.ErrorCode)
                        .SetSqlState(resp.Status.SqlState);
                }

                TRowSet rowSet = GetRowSetAsync(resp, cancellationToken).Result;
                StringArray tableTypes = rowSet.Columns[0].StringVal.Values;

                StringArray.Builder tableTypesBuilder = new StringArray.Builder();
                tableTypesBuilder.AppendRange(tableTypes);

                IArrowArray[] dataArrays = new IArrowArray[]
                {
                tableTypesBuilder.Build()
                };

                return new HiveInfoArrowStream(StandardSchemas.TableTypesSchema, dataArrays);
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ex.Message}'", ex);
            }
        }

        protected async Task<TRowSet> FetchResultsAsync(TOperationHandle operationHandle, long batchSize = BatchSizeDefault, CancellationToken cancellationToken = default)
        {
            await PollForResponseAsync(operationHandle, Client, PollTimeMillisecondsDefault, cancellationToken);

            TFetchResultsResp fetchResp = await FetchNextAsync(operationHandle, Client, batchSize, cancellationToken);
            if (fetchResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(fetchResp.Status.ErrorMessage)
                    .SetNativeError(fetchResp.Status.ErrorCode)
                    .SetSqlState(fetchResp.Status.SqlState);
            }
            return fetchResp.Results;
        }

        private static async Task<TFetchResultsResp> FetchNextAsync(TOperationHandle operationHandle, TCLIService.IAsync client, long batchSize, CancellationToken cancellationToken = default)
        {
            TFetchResultsReq request = new(operationHandle, TFetchOrientation.FETCH_NEXT, batchSize);
            TFetchResultsResp response = await client.FetchResults(request, cancellationToken);
            return response;
        }

        internal struct TableInfo(string type)
        {
            public string Type { get; } = type;

            public List<string> ColumnName { get; } = new();

            public List<short> ColType { get; } = new();

            public List<string> BaseTypeName { get; } = new();

            public List<string> TypeName { get; } = new();

            public List<short> Nullable { get; } = new();

            public List<int?> Precision { get; } = new();

            public List<short?> Scale { get; } = new();

            public List<int> OrdinalPosition { get; } = new();

            public List<string> ColumnDefault { get; } = new();

            public List<string> IsNullable { get; } = new();

            public List<bool> IsAutoIncrement { get; } = new();
        }

        internal class HiveInfoArrowStream : IArrowArrayStream
        {
            private Schema schema;
            private RecordBatch? batch;

            public HiveInfoArrowStream(Schema schema, IReadOnlyList<IArrowArray> data)
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
}
