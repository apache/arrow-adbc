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
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Thrift;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Adbc.Telemetry.Traces.Exporters;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using OpenTelemetry.Trace;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal abstract class HiveServer2Connection : TracingConnection
    {
        internal const bool InfoVendorSql = true;
        internal const long BatchSizeDefault = 50000;
        internal const int PollTimeMillisecondsDefault = 500;
        internal static readonly string s_assemblyName = ApacheUtility.GetAssemblyName(typeof(HiveServer2Connection));
        internal static readonly string s_assemblyVersion = ApacheUtility.GetAssemblyVersion(typeof(HiveServer2Connection));
        private const int ConnectTimeoutMillisecondsDefault = 30000;
        private TTransport? _transport;
        private TCLIService.IAsync? _client;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;
        private bool _isDisposed;
        private static readonly object s_tracerProviderLock = new object();
        private static TracerProvider? s_tracerProvider;
        private static bool s_isFileExporterEnabled = false;

        readonly AdbcInfoCode[] infoSupportedCodes = [
            AdbcInfoCode.DriverName,
            AdbcInfoCode.DriverVersion,
            AdbcInfoCode.DriverArrowVersion,
            AdbcInfoCode.VendorName,
            AdbcInfoCode.VendorSql,
            AdbcInfoCode.VendorVersion,
        ];

        internal const string ColumnDef = "COLUMN_DEF";
        internal const string ColumnName = "COLUMN_NAME";
        internal const string DataType = "DATA_TYPE";
        internal const string IsAutoIncrement = "IS_AUTO_INCREMENT";
        internal const string IsNullable = "IS_NULLABLE";
        internal const string OrdinalPosition = "ORDINAL_POSITION";
        internal const string TableCat = "TABLE_CAT";
        internal const string TableCatalog = "TABLE_CATALOG";
        internal const string TableName = "TABLE_NAME";
        internal const string TableSchem = "TABLE_SCHEM";
        internal const string TableMd = "TABLE_MD";
        internal const string TableType = "TABLE_TYPE";
        internal const string TypeName = "TYPE_NAME";
        internal const string Nullable = "NULLABLE";
        internal const string ColumnSize = "COLUMN_SIZE";
        internal const string DecimalDigits = "DECIMAL_DIGITS";
        internal const string BufferLength = "BUFFER_LENGTH";

        /// <summary>
        /// The GetColumns metadata call returns a result with different column names
        /// on different data sources. Populate this structure with the actual column names.
        /// </summary>
        internal struct ColumnsMetadataColumnNames
        {
            public string TableCatalog { get; internal set; }
            public string TableSchema { get; internal set; }
            public string TableName { get; internal set; }
            public string ColumnName { get; internal set; }
            public string DataType { get; internal set; }
            public string TypeName { get; internal set; }
            public string Nullable { get; internal set; }
            public string ColumnDef { get; internal set; }
            public string OrdinalPosition { get; internal set; }
            public string IsNullable { get; internal set; }
            public string IsAutoIncrement { get; internal set; }
            public string ColumnSize { get; set; }
            public string DecimalDigits { get; set; }
        }

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
            : base(properties)
        {
            Properties = properties;

            TryInitTracerProvider();

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

        private void TryInitTracerProvider()
        {
            // Avoid locking if the tracer provider is already set.
            if (s_tracerProvider != null)
            {
                return;
            }
            // Avoid locking if the exporter option would not activate.
            Properties.TryGetValue(ExportersOptions.Exporter, out string? exporterOption);
            ExportersBuilder exportersBuilder = ExportersBuilder.Build(this.ActivitySourceName, addDefaultExporters: true).Build();
            if (!exportersBuilder.WouldActivate(exporterOption))
            {
                return;
            }

            // Will likely activate the exporter, so we need to lock to ensure thread safety.
            lock (s_tracerProviderLock)
            {
                // Due to race conditions, we need to check again if the tracer provider is already set.
                if (s_tracerProvider != null)
                {
                    return;
                }

                // Activates the exporter specified in the connection property (if exists) or environment variable (if is set).
                if (exportersBuilder.TryActivate(exporterOption, out string? exporterName, out TracerProvider? tracerProvider, ExportersOptions.Environment.Exporter) && tracerProvider != null)
                {
                    s_tracerProvider = tracerProvider;
                    s_isFileExporterEnabled = ExportersOptions.Exporters.AdbcFile.Equals(exporterName);
                }
            }
        }

        /// <summary>
        /// Conditional used to determines if it is safe to trace
        /// </summary>
        /// <remarks>
        /// It is safe to write to some output types (ie, files) but not others (ie, a shared resource).
        /// </remarks>
        /// <returns></returns>
        internal static bool IsSafeToTrace => s_isFileExporterEnabled;

        internal TCLIService.IAsync Client
        {
            get { return _client ?? throw new InvalidOperationException("connection not open"); }
        }

        internal string VendorVersion => _vendorVersion.Value;

        internal string VendorName => _vendorName.Value;

        protected internal int QueryTimeoutSeconds { get; set; } = ApacheUtility.QueryTimeoutSecondsDefault;

        internal IReadOnlyDictionary<string, string> Properties { get; }

        internal async Task OpenAsync()
        {
            await this.TraceActivity(async activity =>
            {
                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(ConnectTimeoutMilliseconds, ApacheUtility.TimeUnit.Milliseconds);
                try
                {
                    TTransport transport = CreateTransport();
                    TProtocol protocol = await CreateProtocolAsync(transport, cancellationToken);
                    _transport = protocol.Transport;
                    _client = CreateTCLIServiceClient(protocol);
                    TOpenSessionReq request = CreateSessionRequest();
                    TOpenSessionResp? session = null;
                    try
                    {
                        session = await Client.OpenSession(request, cancellationToken);
                    }
                    catch (Exception)
                    {
                        if (FallbackProtocolVersions.Any())
                        {
                            session = await TryOpenSessionWithFallbackAsync(request, cancellationToken);
                        }
                        else
                        {
                            throw;
                        }
                    }
                    await HandleOpenSessionResponse(session, activity);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The operation timed out while attempting to open a session. Please try increasing connect timeout.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    // Handle other exceptions if necessary
                    throw new HiveServer2Exception($"An unexpected error occurred while opening the session. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            });
        }

        private async Task<TOpenSessionResp?> TryOpenSessionWithFallbackAsync(TOpenSessionReq originalRequest, CancellationToken cancellationToken)
        {
            Exception? lastException = null;

            foreach (var fallbackVersion in FallbackProtocolVersions)
            {
                try
                {
                    ResetConnection();
                    // Recreate transport + client
                    var retryTransport = CreateTransport();
                    var retryProtocol = await CreateProtocolAsync(retryTransport, cancellationToken);
                    _transport = retryProtocol.Transport;
                    _client = CreateTCLIServiceClient(retryProtocol);
                    // New request with fallback version
                    var retryReq = CreateSessionRequest();
                    retryReq.Client_protocol = fallbackVersion;

                    return await Client.OpenSession(retryReq, cancellationToken);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The operation timed out while attempting to open a session. Please try increasing connect timeout.", ex);
                }
                catch (Exception ex)
                {
                    lastException = ex;
                }
            }

            throw lastException ?? new HiveServer2Exception("Error occurred while opening the session. All protocol fallback attempts failed.");
        }

        private void ResetConnection()
        {
            try
            {
                _transport?.Close();
            }
            catch
            {
                // Ignore cleanup failure
            }

            _transport = null;
            _client = null;
        }

        protected virtual Task HandleOpenSessionResponse(TOpenSessionResp? session, Activity? activity = default)
        {
            // Explicitly check the session status
            if (session == null)
            {
                throw new HiveServer2Exception("Unable to open session. Unknown error.");
            }
            HandleThriftResponse(session.Status, activity);

            SessionHandle = session.SessionHandle;
            ServerProtocolVersion = session.ServerProtocolVersion;
            return Task.CompletedTask;
        }

        protected virtual TCLIService.IAsync CreateTCLIServiceClient(TProtocol protocol)
        {
            return new TCLIService.Client(protocol);
        }

        protected virtual IEnumerable<TProtocolVersion> FallbackProtocolVersions => Enumerable.Empty<TProtocolVersion>();

        internal TSessionHandle? SessionHandle { get; private set; }

        internal TProtocolVersion? ServerProtocolVersion { get; private set; }

        protected internal DataTypeConversion DataTypeConversion { get; set; } = DataTypeConversion.None;

        protected internal TlsProperties TlsOptions { get; set; } = new TlsProperties();

        protected internal int ConnectTimeoutMilliseconds { get; set; } = ConnectTimeoutMillisecondsDefault;

        protected abstract TTransport CreateTransport();

        protected abstract Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default);

        protected abstract TOpenSessionReq CreateSessionRequest();

        internal abstract SchemaParser SchemaParser { get; }

        internal abstract IArrowArrayStream NewReader<T>(
            T statement,
            Schema schema,
            IResponse response,
            TGetResultSetMetadataResp? metadataResp = null) where T : IHiveServer2Statement;

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            return this.TraceActivity(_ =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>>();
                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    if (GetObjectsPatternsRequireLowerCase)
                    {
                        catalogPattern = catalogPattern?.ToLower();
                        dbSchemaPattern = dbSchemaPattern?.ToLower();
                        tableNamePattern = tableNamePattern?.ToLower();
                        columnNamePattern = columnNamePattern?.ToLower();
                    }
                    if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
                    {
                        TGetCatalogsResp getCatalogsResp = GetCatalogsAsync(cancellationToken).Result;

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
                        TGetSchemasResp getSchemasResp = GetSchemasAsync(catalogPattern, dbSchemaPattern, cancellationToken).Result;

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
                        TGetTablesResp getTablesResp = GetTablesAsync(
                            catalogPattern,
                            dbSchemaPattern,
                            tableNamePattern,
                            tableTypes?.ToList(),
                            cancellationToken).Result;

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
                        TGetColumnsResp columnsResponse = GetColumnsAsync(
                            catalogPattern,
                            dbSchemaPattern,
                            tableNamePattern,
                            columnNamePattern,
                            cancellationToken).Result;

                        TGetResultSetMetadataResp columnsMetadata = GetResultSetMetadataAsync(columnsResponse, cancellationToken).Result;
                        IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(columnsMetadata.Schema.Columns);
                        TRowSet rowSet = GetRowSetAsync(columnsResponse, cancellationToken).Result;

                        ColumnsMetadataColumnNames columnNames = GetColumnsMetadataColumnNames();
                        IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[columnNames.TableCatalog]].StringVal.Values;
                        IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[columnNames.TableSchema]].StringVal.Values;
                        IReadOnlyList<string> tableList = rowSet.Columns[columnMap[columnNames.TableName]].StringVal.Values;
                        IReadOnlyList<string> columnNameList = rowSet.Columns[columnMap[columnNames.ColumnName]].StringVal.Values;
                        ReadOnlySpan<int> columnTypeList = rowSet.Columns[columnMap[columnNames.DataType]].I32Val.Values.Values;
                        IReadOnlyList<string> typeNameList = rowSet.Columns[columnMap[columnNames.TypeName]].StringVal.Values;
                        ReadOnlySpan<int> nullableList = rowSet.Columns[columnMap[columnNames.Nullable]].I32Val.Values.Values;
                        IReadOnlyList<string> columnDefaultList = rowSet.Columns[columnMap[columnNames.ColumnDef]].StringVal.Values;
                        ReadOnlySpan<int> ordinalPosList = rowSet.Columns[columnMap[columnNames.OrdinalPosition]].I32Val.Values.Values;
                        IReadOnlyList<string> isNullableList = rowSet.Columns[columnMap[columnNames.IsNullable]].StringVal.Values;
                        IReadOnlyList<string> isAutoIncrementList = rowSet.Columns[columnMap[columnNames.IsAutoIncrement]].StringVal.Values;
                        ReadOnlySpan<int> columnSizeList = rowSet.Columns[columnMap[columnNames.ColumnSize]].I32Val.Values.Values;
                        ReadOnlySpan<int> decimalDigitsList = rowSet.Columns[columnMap[columnNames.DecimalDigits]].I32Val.Values.Values;

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
                            int ordinalPos = ordinalPosList[i] + PositionRequiredOffset;
                            int columnSize = columnSizeList[i];
                            int decimalDigits = decimalDigitsList[i];
                            TableInfo? tableInfo = catalogMap.GetValueOrDefault(catalog)?.GetValueOrDefault(schemaDb)?.GetValueOrDefault(tableName);
                            tableInfo?.ColumnName.Add(columnName);
                            tableInfo?.ColType.Add(colType);
                            tableInfo?.Nullable.Add(nullable);
                            tableInfo?.IsAutoIncrement.Add(isAutoIncrement);
                            tableInfo?.IsNullable.Add(isNullable);
                            tableInfo?.ColumnDefault.Add(columnDefault);
                            tableInfo?.OrdinalPosition.Add(ordinalPos);
                            SetPrecisionScaleAndTypeName(colType, typeName, tableInfo, columnSize, decimalDigits);
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
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            });
        }

        public override IArrowArrayStream GetTableTypes()
        {
            return this.TraceActivity(activity =>
            {
                TGetTableTypesReq req = new()
                {
                    SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
                };
                TrySetGetDirectResults(req);

                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    TGetTableTypesResp resp = Client.GetTableTypes(req, cancellationToken).Result;
                    HandleThriftResponse(resp.Status, activity);

                    TRowSet rowSet = GetRowSetAsync(resp, cancellationToken).Result;
                    StringArray tableTypes = rowSet.Columns[0].StringVal.Values;

                    HashSet<string> distinctTableTypes = new HashSet<string>(tableTypes);

                    StringArray.Builder tableTypesBuilder = new StringArray.Builder();
                    tableTypesBuilder.AppendRange(distinctTableTypes);

                    IArrowArray[] dataArrays = new IArrowArray[]
                    {
                tableTypesBuilder.Build()
                    };

                    return new HiveInfoArrowStream(StandardSchemas.TableTypesSchema, dataArrays);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            });
        }

        internal static async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds, CancellationToken cancellationToken = default)
        {
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { await Task.Delay(pollTimeMilliseconds, cancellationToken); }
                TGetOperationStatusReq request = new(operationHandle);
                statusResponse = await client.GetOperationStatus(request, cancellationToken);
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);

            // Must be in the finished state to be valid. If not, typically a server error or timeout has occurred.
            if (statusResponse.OperationState != TOperationState.FINISHED_STATE)
            {
#pragma warning disable CS0618 // Type or member is obsolete
                throw new HiveServer2Exception(statusResponse.ErrorMessage, AdbcStatusCode.InvalidState)
                    .SetSqlState(statusResponse.SqlState)
                    .SetNativeError(statusResponse.ErrorCode);
#pragma warning restore CS0618 // Type or member is obsolete
            }
        }

        private string GetInfoTypeStringValue(TGetInfoType infoType)
        {
            return this.TraceActivity(activity =>
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
                    HandleThriftResponse(getInfoResp.Status, activity);

                    return getInfoResp.InfoValue.StringValue;
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            });
        }

        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed && disposing)
            {
                DisposeClient();
                _isDisposed = true;
                s_tracerProvider?.ForceFlush();
            }
            base.Dispose(disposing);
        }

        private void DisposeClient()
        {
            this.TraceActivity(activity =>
            {
                if (_client != null && SessionHandle != null)
                {
                    CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);

                    TCloseSessionReq r6 = new(SessionHandle);
                    var resp = _client.CloseSession(r6, cancellationToken).Result;
                    HandleThriftResponse(resp.Status, activity);

                    _transport?.Close();
                    if (_client is IDisposable disposableClient)
                    {
                        disposableClient.Dispose();
                    }
                    _transport = null;
                    _client = null;
                }
            });
        }

        internal static async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataReq request = new(operationHandle);
            TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
            return response;
        }

        /// <summary>
        /// Gets the data-source specific columns names for the GetColumns metadata result.
        /// </summary>
        /// <returns></returns>
        protected abstract ColumnsMetadataColumnNames GetColumnsMetadataColumnNames();

        /// <summary>
        /// Gets the default product version
        /// </summary>
        /// <returns></returns>
        protected abstract string GetProductVersionDefault();

        /// <summary>
        /// Gets the current product version.
        /// </summary>
        /// <returns></returns>
        protected internal string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? GetProductVersionDefault();
        }

        protected static Uri GetBaseAddress(string? uri, string? hostName, string? path, string? port, string hostOptionName, bool isTlsEnabled)
        {
            // Uri property takes precedent.
            if (!string.IsNullOrWhiteSpace(uri))
            {
                if (!string.IsNullOrWhiteSpace(hostName))
                {
                    throw new ArgumentOutOfRangeException(
                        AdbcOptions.Uri,
                        hostOptionName,
                        $"Conflicting server arguments. Please provide only one of the following options: '{Adbc.AdbcOptions.Uri}' or '{hostOptionName}'.");
                }

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
            string uriScheme = isTlsEnabled ? Uri.UriSchemeHttps : Uri.UriSchemeHttp;
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

        internal IReadOnlyDictionary<string, int> GetColumnIndexMap(List<TColumnDesc> columns) => columns
           .Select(t => new { Index = t.Position - ColumnMapIndexOffset, t.ColumnName })
           .ToDictionary(t => t.ColumnName, t => t.Index);

        protected abstract int ColumnMapIndexOffset { get; }

        protected abstract Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(IResponse response, CancellationToken cancellationToken = default);

        protected abstract Task<TRowSet> GetRowSetAsync(IResponse response, CancellationToken cancellationToken = default);

        protected internal virtual bool TrySetGetDirectResults(IRequest request) => false;

        protected internal virtual bool TryGetDirectResults(TSparkDirectResults? directResults, [MaybeNullWhen(false)] out QueryResult result)
        {
            result = null;
            return false;
        }

        protected internal virtual bool TryGetDirectResults(
            TSparkDirectResults? directResults,
            [MaybeNullWhen(false)] out TGetResultSetMetadataResp metadata,
            [MaybeNullWhen(false)] out TRowSet rowSet)
        {
            metadata = null;
            rowSet = null;
            return false;
        }

        protected internal abstract int PositionRequiredOffset { get; }

        protected abstract string InfoDriverName { get; }

        protected abstract string InfoDriverArrowVersion { get; }

        protected abstract string ProductVersion { get; }

        protected abstract bool GetObjectsPatternsRequireLowerCase { get; }

        protected abstract bool IsColumnSizeValidForDecimal { get; }

        public override void SetOption(string key, string? value)
        {
            // These option can be set even if already connected.
            switch (key.ToLowerInvariant())
            {
                case AdbcOptions.Telemetry.TraceParent:
                    SetTraceParent(string.IsNullOrWhiteSpace(value) ? null : value);
                    return;
            }

            if (SessionHandle != null)
            {
                throw new AdbcException($"Option '{key}' cannot be set once the connection is open.", AdbcStatusCode.InvalidState);
            }

            // These option can only be set before connection is open.
            switch (key.ToLowerInvariant())
            {
                default:
                    throw AdbcException.NotImplemented($"Option '{key}' is not implemented");
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

        internal async Task<TGetCatalogsResp> GetCatalogsAsync(CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetCatalogsReq req = new TGetCatalogsReq(SessionHandle);
                TrySetGetDirectResults(req);

                TGetCatalogsResp resp = await Client.GetCatalogs(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            });
        }

        internal async Task<TGetSchemasResp> GetSchemasAsync(
            string? catalogName,
            string? schemaName,
            CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetSchemasReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName;
                }

                TGetSchemasResp resp = await Client.GetSchemas(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            });
        }

        internal async Task<TGetTablesResp> GetTablesAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            List<string>? tableTypes,
            CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetTablesReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName;
                }
                if (tableName != null)
                {
                    req.TableName = tableName;
                }
                if (tableTypes != null && tableTypes.Count > 0)
                {
                    req.TableTypes = tableTypes;
                }

                TGetTablesResp resp = await Client.GetTables(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            });
        }

        internal async Task<TGetColumnsResp> GetColumnsAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            string? columnName,
            CancellationToken cancellationToken)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetColumnsReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName;
                }
                if (tableName != null)
                {
                    req.TableName = tableName;
                }
                if (columnName != null)
                {
                    req.ColumnName = columnName;
                }

                TGetColumnsResp resp = await Client.GetColumns(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            });
        }

        internal async Task<TGetPrimaryKeysResp> GetPrimaryKeysAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetPrimaryKeysReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.CatalogName = catalogName!;
                }
                if (schemaName != null)
                {
                    req.SchemaName = schemaName!;
                }
                if (tableName != null)
                {
                    req.TableName = tableName!;
                }

                TGetPrimaryKeysResp resp = await Client.GetPrimaryKeys(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);

                return resp;
            });
        }

        internal async Task<TGetCrossReferenceResp> GetCrossReferenceAsync(
            string? catalogName,
            string? schemaName,
            string? tableName,
            string? foreignCatalogName,
            string? foreignSchemaName,
            string? foreignTableName,
            CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetCrossReferenceReq req = new(SessionHandle);
                TrySetGetDirectResults(req);
                if (catalogName != null)
                {
                    req.ParentCatalogName = catalogName!;
                }
                if (schemaName != null)
                {
                    req.ParentSchemaName = schemaName!;
                }
                if (tableName != null)
                {
                    req.ParentTableName = tableName!;
                }
                if (foreignCatalogName != null)
                {
                    req.ForeignCatalogName = foreignCatalogName!;
                }
                if (foreignSchemaName != null)
                {
                    req.ForeignSchemaName = foreignSchemaName!;
                }
                if (foreignTableName != null)
                {
                    req.ForeignTableName = foreignTableName!;
                }

                TGetCrossReferenceResp resp = await Client.GetCrossReference(req, cancellationToken);
                HandleThriftResponse(resp.Status, activity);
                return resp;
            });
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

        internal abstract void SetPrecisionScaleAndTypeName(short columnType, string typeName, TableInfo? tableInfo, int columnSize, int decimalDigits);

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string? tableName)
        {
            return this.TraceActivity(activity =>
            {
                if (SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                TGetColumnsReq getColumnsReq = new TGetColumnsReq(SessionHandle);
                getColumnsReq.CatalogName = catalog;
                getColumnsReq.SchemaName = dbSchema;
                getColumnsReq.TableName = tableName;
                TrySetGetDirectResults(getColumnsReq);

                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    var columnsResponse = Client.GetColumns(getColumnsReq, cancellationToken).Result;
                    HandleThriftResponse(columnsResponse.Status, activity);

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
                        bool isColumnSizeValid = IsColumnSizeValidForDecimal;
                        int? columnSize = columns[6].I32Val.Values.GetValue(i);
                        int? decimalDigits = columns[8].I32Val.Values.GetValue(i);
                        bool nullable = columns[10].I32Val.Values.GetValue(i) == 1;
                        IArrowType dataType = HiveServer2Connection.GetArrowType(columnType!.Value, typeName, isColumnSizeValid, columnSize, decimalDigits);
                        fields[i] = new Field(columnName, dataType, nullable);
                    }
                    return new Schema(fields, null);
                }
                catch (Exception ex) when (ExceptionHelper.IsOperationCanceledOrCancellationRequested(ex, cancellationToken))
                {
                    throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            });
        }

        private static IArrowType GetArrowType(int columnTypeId, string typeName, bool isColumnSizeValid, int? columnSize, int? decimalDigits)
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
                    if (isColumnSizeValid && columnSize.HasValue && decimalDigits.HasValue)
                    {
                        return new Decimal128Type(columnSize.Value, decimalDigits.Value);
                    }
                    else
                    {
                        // Note: parsing the type name for SQL DECIMAL types as the precision and scale values
                        // may not be returned in the Thrift call to GetColumns
                        return SqlTypeNameParser<SqlDecimalParserResult>
                            .Parse(typeName, columnTypeId)
                            .Decimal128Type;
                    }
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

        internal async Task<TRowSet> FetchResultsAsync(TOperationHandle operationHandle, long batchSize = BatchSizeDefault, CancellationToken cancellationToken = default)
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

        public override IArrowArrayStream GetInfo(IReadOnlyList<AdbcInfoCode> codes)
        {
            return this.TraceActivity(activity =>
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
                    string tagKey = SemanticConventions.Db.Operation.Parameter(code.ToString().ToLowerInvariant());
                    Func<object?> tagValue = () => null;
                    switch (code)
                    {
                        case AdbcInfoCode.DriverName:
                            infoNameBuilder.Append((UInt32)code);
                            typeBuilder.Append(strValTypeID);
                            offsetBuilder.Append(offset++);
                            stringInfoBuilder.Append(InfoDriverName);
                            booleanInfoBuilder.AppendNull();
                            tagValue = () => InfoDriverName;
                            break;
                        case AdbcInfoCode.DriverVersion:
                            infoNameBuilder.Append((UInt32)code);
                            typeBuilder.Append(strValTypeID);
                            offsetBuilder.Append(offset++);
                            stringInfoBuilder.Append(ProductVersion);
                            booleanInfoBuilder.AppendNull();
                            tagValue = () => ProductVersion;
                            break;
                        case AdbcInfoCode.DriverArrowVersion:
                            infoNameBuilder.Append((UInt32)code);
                            typeBuilder.Append(strValTypeID);
                            offsetBuilder.Append(offset++);
                            stringInfoBuilder.Append(InfoDriverArrowVersion);
                            booleanInfoBuilder.AppendNull();
                            tagValue = () => InfoDriverArrowVersion;
                            break;
                        case AdbcInfoCode.VendorName:
                            infoNameBuilder.Append((UInt32)code);
                            typeBuilder.Append(strValTypeID);
                            offsetBuilder.Append(offset++);
                            string vendorName = VendorName;
                            stringInfoBuilder.Append(vendorName);
                            booleanInfoBuilder.AppendNull();
                            tagValue = () => vendorName;
                            break;
                        case AdbcInfoCode.VendorVersion:
                            infoNameBuilder.Append((UInt32)code);
                            typeBuilder.Append(strValTypeID);
                            offsetBuilder.Append(offset++);
                            string? vendorVersion = VendorVersion;
                            stringInfoBuilder.Append(vendorVersion);
                            booleanInfoBuilder.AppendNull();
                            tagValue = () => vendorVersion;
                            break;
                        case AdbcInfoCode.VendorSql:
                            infoNameBuilder.Append((UInt32)code);
                            typeBuilder.Append(boolValTypeId);
                            offsetBuilder.Append(offset++);
                            stringInfoBuilder.AppendNull();
                            booleanInfoBuilder.Append(InfoVendorSql);
                            tagValue = () => InfoVendorSql;
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
                    Tracing.ActivityExtensions.AddTag(activity, tagKey, tagValue);
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

                return new HiveInfoArrowStream(StandardSchemas.GetInfoSchema, dataArrays);
            });
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

        private static class ExceptionHelper
        {
            public static bool IsOperationCanceledOrCancellationRequested(Exception ex, CancellationToken cancellationToken)
            {
                return ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                       (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested);
            }
        }

        internal static void HandleThriftResponse(TStatus status, Activity? activity)
        {
            if (ErrorHandlers.TryGetValue(status.StatusCode, out Action<TStatus, Activity?>? handler))
            {
                handler(status, activity);
            }
        }

        private static IReadOnlyDictionary<TStatusCode, Action<TStatus, Activity?>> ErrorHandlers => new Dictionary<TStatusCode, Action<TStatus, Activity?>>()
        {
            [TStatusCode.ERROR_STATUS] = (status, _) => ThrowErrorResponse(status),
            [TStatusCode.INVALID_HANDLE_STATUS] = (status, _) => ThrowErrorResponse(status),
            [TStatusCode.STILL_EXECUTING_STATUS] = (status, _) => ThrowErrorResponse(status, AdbcStatusCode.InvalidState),
            [TStatusCode.SUCCESS_STATUS] = (status, activity) => activity?.AddTag(SemanticConventions.Db.Response.StatusCode, status.StatusCode),
            [TStatusCode.SUCCESS_WITH_INFO_STATUS] = (status, activity) =>
            {
                activity?.AddTag(SemanticConventions.Db.Response.StatusCode, status.StatusCode);
                activity?.AddTag(SemanticConventions.Db.Response.InfoMessages, string.Join(Environment.NewLine, status.InfoMessages));
            },
        };

        private static void ThrowErrorResponse(TStatus status, AdbcStatusCode adbcStatusCode = AdbcStatusCode.InternalError) =>
            throw new HiveServer2Exception(status.ErrorMessage, adbcStatusCode)
                .SetSqlState(status.SqlState)
                .SetNativeError(status.ErrorCode);
    }
}
