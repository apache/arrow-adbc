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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2Statement : TracingStatement
    {
        private const string GetPrimaryKeysCommandName = "getprimarykeys";
        private const string GetCrossReferenceCommandName = "getcrossreference";
        private const string GetCatalogsCommandName = "getcatalogs";
        private const string GetSchemasCommandName = "getschemas";
        private const string GetTablesCommandName = "gettables";
        private const string GetColumnsCommandName = "getcolumns";
        private const string GetColumnsExtendedCommandName = "getcolumnsextended";
        private const string SupportedMetadataCommands =
            GetCatalogsCommandName + "," +
            GetSchemasCommandName + "," +
            GetTablesCommandName + "," +
            GetColumnsCommandName + "," +
            GetPrimaryKeysCommandName + "," +
            GetCrossReferenceCommandName + "," +
            GetColumnsExtendedCommandName;

        // Add constants for PK and FK field names and prefixes
        protected static readonly string[] PrimaryKeyFields = new[] { "COLUMN_NAME" };
        protected static readonly string[] ForeignKeyFields = new[] { "PKCOLUMN_NAME", "PKTABLE_CAT", "PKTABLE_SCHEM", "PKTABLE_NAME", "FKCOLUMN_NAME", "FK_NAME", "KEQ_SEQ" };
        protected const string PrimaryKeyPrefix = "PK_";
        protected const string ForeignKeyPrefix = "FK_";

        internal HiveServer2Statement(HiveServer2Connection connection)
            : base(connection)
        {
            Connection = connection;
            ValidateOptions(connection.Properties);
        }

        protected virtual void SetStatementProperties(TExecuteStatementReq statement)
        {
            statement.QueryTimeout = QueryTimeoutSeconds;
        }

        /// <summary>
        /// Gets the schema from metadata response. Base implementation uses traditional Thrift schema.
        /// Subclasses can override to support Arrow schema parsing.
        /// </summary>
        /// <param name="metadata">The metadata response containing schema information</param>
        /// <returns>The Arrow schema</returns>
        protected virtual Schema GetSchemaFromMetadata(TGetResultSetMetadataResp metadata)
        {
            return Connection.SchemaParser.GetArrowSchema(metadata.Schema, Connection.DataTypeConversion);
        }

        public override QueryResult ExecuteQuery()
        {
            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                return ExecuteQueryAsyncInternal(cancellationToken).Result;
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
            }
        }

        public override UpdateResult ExecuteUpdate()
        {
            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                return ExecuteUpdateAsyncInternal(cancellationToken).Result;
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
            }
        }

        private async Task<QueryResult> ExecuteQueryAsyncInternal(CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async _ =>
            {
                // Clear any previous query result
                ClearCurrentQueryResult();

                if (IsMetadataCommand)
                {
                    _currentQueryResult = await ExecuteMetadataCommandQuery(cancellationToken);
                    return _currentQueryResult;
                }

                _directResults = null;

                // this could either:
                // take QueryTimeoutSeconds * 3
                // OR
                // take QueryTimeoutSeconds (but this could be restricting)
                await ExecuteStatementAsync(cancellationToken); // --> get QueryTimeout +

                TGetResultSetMetadataResp metadata;
                if (_directResults?.OperationStatus?.OperationState == TOperationState.FINISHED_STATE)
                {
                    // The initial response has result data so we don't need to poll
                    metadata = _directResults.ResultSetMetadata;
                }
                else
                {
                    await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds, cancellationToken); // + poll, up to QueryTimeout
                    metadata = await HiveServer2Connection.GetResultSetMetadataAsync(OperationHandle!, Connection.Client, cancellationToken);
                }
                Schema schema = GetSchemaFromMetadata(metadata);
                _currentQueryResult = new QueryResult(-1, Connection.NewReader(this, schema, metadata));
                return _currentQueryResult;
            });
        }

        public override async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                return await ExecuteQueryAsyncInternal(cancellationToken);
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
            }
        }

        private async Task<UpdateResult> ExecuteUpdateAsyncInternal(CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                long? affectedRows = null;
                try
                {
                    const string NumberOfAffectedRowsColumnName = "num_affected_rows";
                    QueryResult queryResult = await ExecuteQueryAsyncInternal(cancellationToken);
                    if (queryResult.Stream == null)
                    {
                        throw new AdbcException("no data found");
                    }

                    using IArrowArrayStream stream = queryResult.Stream;

                    // Check if the affected rows columns are returned in the result.
                    Field affectedRowsField = stream.Schema.GetFieldByName(NumberOfAffectedRowsColumnName);
                    if (affectedRowsField != null && affectedRowsField.DataType.TypeId != Types.ArrowTypeId.Int64)
                    {
                        throw new AdbcException($"Unexpected data type for column: '{NumberOfAffectedRowsColumnName}'", new ArgumentException(NumberOfAffectedRowsColumnName));
                    }

                    // The default is -1.
                    if (affectedRowsField == null)
                    {
                        affectedRows = -1;
                        return new UpdateResult(affectedRows.Value);
                    }

                    while (true)
                    {
                        using RecordBatch nextBatch = await stream.ReadNextRecordBatchAsync(cancellationToken);
                        if (nextBatch == null) { break; }
                        Int64Array numOfModifiedArray = (Int64Array)nextBatch.Column(NumberOfAffectedRowsColumnName);
                        // Note: should only have one item, but iterate for completeness
                        for (int i = 0; i < numOfModifiedArray.Length; i++)
                        {
                            // Note: handle the case where the affected rows are zero (0).
                            affectedRows = (affectedRows ?? 0) + numOfModifiedArray.GetValue(i).GetValueOrDefault(0);
                        }
                    }

                    // If no altered rows, i.e. DDC statements, then -1 is the default.
                    affectedRows ??= -1;
                    return new UpdateResult(affectedRows.Value);
                }
                finally
                {
                    activity?.AddTag(SemanticConventions.Db.Response.ReturnedRows, affectedRows ?? -1);
                }
            });
        }

        public override async Task<UpdateResult> ExecuteUpdateAsync()
        {
            return await this.TraceActivityAsync(async _ =>
            {
                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                try
                {
                    return await ExecuteUpdateAsyncInternal(cancellationToken);
                }
                catch (Exception ex)
                    when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                         (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
                {
                    throw new TimeoutException("The query execution timed out. Consider increasing the query timeout value.", ex);
                }
                catch (Exception ex) when (ex is not HiveServer2Exception)
                {
                    throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ApacheUtility.FormatExceptionMessage(ex)}'", ex);
                }
            });
        }

        public override void SetOption(string key, string value)
        {
            switch (key)
            {
                case ApacheParameters.PollTimeMilliseconds:
                    UpdatePollTimeIfValid(key, value);
                    break;
                case ApacheParameters.BatchSize:
                    UpdateBatchSizeIfValid(key, value);
                    break;
                case ApacheParameters.QueryTimeoutSeconds:
                    if (ApacheUtility.QueryTimeoutIsValid(key, value, out int queryTimeoutSeconds))
                    {
                        QueryTimeoutSeconds = queryTimeoutSeconds;
                    }
                    break;
                case ApacheParameters.IsMetadataCommand:
                    if (ApacheUtility.BooleanIsValid(key, value, out bool isMetadataCommand))
                    {
                        IsMetadataCommand = isMetadataCommand;
                    }
                    break;
                case ApacheParameters.CatalogName:
                    this.CatalogName = value;
                    break;
                case ApacheParameters.SchemaName:
                    this.SchemaName = value;
                    break;
                case ApacheParameters.TableName:
                    this.TableName = value;
                    break;
                case ApacheParameters.TableTypes:
                    this.TableTypes = value;
                    break;
                case ApacheParameters.ColumnName:
                    this.ColumnName = value;
                    break;
                case ApacheParameters.ForeignCatalogName:
                    this.ForeignCatalogName = value;
                    break;
                case ApacheParameters.ForeignSchemaName:
                    this.ForeignSchemaName = value;
                    break;
                case ApacheParameters.ForeignTableName:
                    this.ForeignTableName = value;
                    break;
                case ApacheParameters.EscapePatternWildcards:
                    if (ApacheUtility.BooleanIsValid(key, value, out bool escapePatternWildcards))
                    {
                        this.EscapePatternWildcards = escapePatternWildcards;
                    }
                    break;
                default:
                    throw AdbcException.NotImplemented($"Option '{key}' is not implemented.");
            }
        }

        protected async Task ExecuteStatementAsync(CancellationToken cancellationToken = default)
        {
            await this.TraceActivityAsync(async activity =>
            {
                if (Connection.SessionHandle == null)
                {
                    throw new InvalidOperationException("Invalid session");
                }

                activity?.AddTag(SemanticConventions.Db.Client.Connection.SessionId, Connection.SessionHandle.SessionId.Guid, "N");
                TExecuteStatementReq executeRequest = new TExecuteStatementReq(Connection.SessionHandle, SqlQuery!);
                SetStatementProperties(executeRequest);
                TExecuteStatementResp executeResponse = await Connection.Client.ExecuteStatement(executeRequest, cancellationToken);
                HiveServer2Connection.HandleThriftResponse(executeResponse.Status, activity);
                activity?.AddTag(SemanticConventions.Db.Response.OperationId, executeResponse.OperationHandle.OperationId.Guid, "N");

                OperationHandle = executeResponse.OperationHandle;

                // Capture direct results if they're available
                if (executeResponse.DirectResults != null)
                {
                    _directResults = executeResponse.DirectResults;

                    if (!string.IsNullOrEmpty(_directResults.OperationStatus?.DisplayMessage))
                    {
                        throw new HiveServer2Exception(_directResults.OperationStatus!.DisplayMessage)
                            .SetSqlState(_directResults.OperationStatus.SqlState)
                            .SetNativeError(_directResults.OperationStatus.ErrorCode);
                    }
                }
            });
        }

        protected internal int PollTimeMilliseconds { get; private set; } = HiveServer2Connection.PollTimeMillisecondsDefault;

        protected internal long BatchSize { get; private set; } = HiveServer2Connection.BatchSizeDefault;

        protected internal int QueryTimeoutSeconds
        {
            // Coordinate updates with the connection
            get => Connection.QueryTimeoutSeconds;
            set => Connection.QueryTimeoutSeconds = value;
        }

        protected internal bool IsMetadataCommand { get; set; } = false;
        protected internal string? CatalogName { get; set; }
        protected internal string? SchemaName { get; set; }
        protected internal string? TableName { get; set; }
        protected internal string? TableTypes { get; set; }
        protected internal string? ColumnName { get; set; }
        protected internal string? ForeignCatalogName { get; set; }
        protected internal string? ForeignSchemaName { get; set; }
        protected internal string? ForeignTableName { get; set; }
        protected internal bool EscapePatternWildcards { get; set; } = false;
        protected internal TSparkDirectResults? _directResults { get; set; }

        // Store the current query result for disposal
        private QueryResult? _currentQueryResult;

        public HiveServer2Connection Connection { get; private set; }

        public TOperationHandle? OperationHandle { get; private set; }

        // Keep the original Client property for internal use
        public TCLIService.IAsync Client => Connection.Client;

        public override string AssemblyName => HiveServer2Connection.s_assemblyName;

        public override string AssemblyVersion => HiveServer2Connection.s_assemblyVersion;

        private void UpdatePollTimeIfValid(string key, string value) => PollTimeMilliseconds = !string.IsNullOrEmpty(key) && int.TryParse(value, result: out int pollTimeMilliseconds) && pollTimeMilliseconds >= 0
            ? pollTimeMilliseconds
            : throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than or equal to 0.");

        private void UpdateBatchSizeIfValid(string key, string value) => BatchSize = !string.IsNullOrEmpty(value) && long.TryParse(value, out long batchSize) && batchSize > 0
            ? batchSize
            : throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than zero.");

        private string? EscapePatternWildcardsInName(string? name)
        {
            if (!EscapePatternWildcards || name == null)
                return name;
            // Escape both _ and %
            return name.Replace("_", "\\_").Replace("%", "\\%");
        }

        public override void Dispose()
        {
            this.TraceActivity(activity =>
            {
                if (OperationHandle != null && _directResults?.CloseOperation?.Status?.StatusCode != TStatusCode.SUCCESS_STATUS)
                {
                    CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                    activity?.AddTag(SemanticConventions.Db.Operation.OperationId, OperationHandle.OperationId.Guid, "N");
                    TCloseOperationReq request = new TCloseOperationReq(OperationHandle);
                    TCloseOperationResp resp = Connection.Client.CloseOperation(request, cancellationToken).Result;
                    HiveServer2Connection.HandleThriftResponse(resp.Status, activity);
                    OperationHandle = null;
                }

                // Dispose the current query result if it exists
                ClearCurrentQueryResult();

                base.Dispose();
            });
        }

        protected void ValidateOptions(IReadOnlyDictionary<string, string> properties)
        {
            foreach (KeyValuePair<string, string> kvp in properties)
            {
                switch (kvp.Key)
                {
                    case ApacheParameters.BatchSize:
                    case ApacheParameters.PollTimeMilliseconds:
                    case ApacheParameters.QueryTimeoutSeconds:
                        {
                            SetOption(kvp.Key, kvp.Value);
                            break;
                        }
                }
            }
        }

        private async Task<QueryResult> ExecuteMetadataCommandQuery(CancellationToken cancellationToken)
        {
            return SqlQuery?.ToLowerInvariant() switch
            {
                GetCatalogsCommandName => await GetCatalogsAsync(cancellationToken),
                GetSchemasCommandName => await GetSchemasAsync(cancellationToken),
                GetTablesCommandName => await GetTablesAsync(cancellationToken),
                GetColumnsCommandName => await GetColumnsAsync(cancellationToken),
                GetPrimaryKeysCommandName => await GetPrimaryKeysAsync(cancellationToken),
                GetCrossReferenceCommandName => await GetCrossReferenceAsync(cancellationToken),
                GetColumnsExtendedCommandName => await GetColumnsExtendedAsync(cancellationToken),
                null or "" => throw new ArgumentNullException(nameof(SqlQuery), $"Metadata command for property 'SqlQuery' must not be empty or null. Supported metadata commands: {SupportedMetadataCommands}"),
                _ => throw new NotSupportedException($"Metadata command '{SqlQuery}' is not supported. Supported metadata commands: {SupportedMetadataCommands}"),
            };
        }
        // This method is for internal use only and is not available for external use.
        // It retrieves cross-reference data where the current table is treated as a foreign table.
        // This is used in GetColumnsExtendedAsync to fetch foreign key relationships.
        /// Note: Unlike other metadata queries, this method does not escape underscores in names
        /// since the backend treats these as exact match queries rather than pattern matches.
        protected virtual async Task<QueryResult> GetCrossReferenceAsForeignTableAsync(CancellationToken cancellationToken = default)
        {
            TGetCrossReferenceResp resp = await Connection.GetCrossReferenceAsync(
                null,
                null,
                null,
                CatalogName,
                SchemaName,
                TableName,
                cancellationToken);
            OperationHandle = resp.OperationHandle;
            _directResults = resp.DirectResults;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        /// <summary>
        /// Gets the cross reference (foreign key) information for the specified tables.
        /// Note: Unlike other metadata queries, this method does not escape underscores in names
        /// since the backend treats these as exact match queries rather than pattern matches.
        /// </summary>
        protected virtual async Task<QueryResult> GetCrossReferenceAsync(CancellationToken cancellationToken = default)
        {
            TGetCrossReferenceResp resp = await Connection.GetCrossReferenceAsync(
                CatalogName,
                SchemaName,
                TableName,
                ForeignCatalogName,
                ForeignSchemaName,
                ForeignTableName,
                cancellationToken);
            OperationHandle = resp.OperationHandle;
            _directResults = resp.DirectResults;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        /// <summary>
        /// Gets the primary key information for the specified table.
        /// Note: Unlike other metadata queries, this method does not escape underscores in names
        /// since the backend treats these as exact match queries rather than pattern matches.
        /// </summary>
        protected virtual async Task<QueryResult> GetPrimaryKeysAsync(CancellationToken cancellationToken = default)
        {
            TGetPrimaryKeysResp resp = await Connection.GetPrimaryKeysAsync(
                CatalogName,
                SchemaName,
                TableName,
                cancellationToken);
            OperationHandle = resp.OperationHandle;
            _directResults = resp.DirectResults;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        protected virtual async Task<QueryResult> GetCatalogsAsync(CancellationToken cancellationToken = default)
        {
            TGetCatalogsResp resp = await Connection.GetCatalogsAsync(cancellationToken);
            OperationHandle = resp.OperationHandle;
            _directResults = resp.DirectResults;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        protected virtual async Task<QueryResult> GetSchemasAsync(CancellationToken cancellationToken = default)
        {
            TGetSchemasResp resp = await Connection.GetSchemasAsync(
                EscapePatternWildcardsInName(CatalogName),
                EscapePatternWildcardsInName(SchemaName),
                cancellationToken);
            OperationHandle = resp.OperationHandle;
            _directResults = resp.DirectResults;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        protected virtual async Task<QueryResult> GetTablesAsync(CancellationToken cancellationToken = default)
        {
            List<string>? tableTypesList = this.TableTypes?.Split(',').ToList();
            TGetTablesResp resp = await Connection.GetTablesAsync(
                EscapePatternWildcardsInName(CatalogName),
                EscapePatternWildcardsInName(SchemaName),
                EscapePatternWildcardsInName(TableName),
                tableTypesList,
                cancellationToken);
            OperationHandle = resp.OperationHandle;
            _directResults = resp.DirectResults;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        protected virtual async Task<QueryResult> GetColumnsAsync(CancellationToken cancellationToken = default)
        {
            TGetColumnsResp resp = await Connection.GetColumnsAsync(
                EscapePatternWildcardsInName(CatalogName),
                EscapePatternWildcardsInName(SchemaName),
                EscapePatternWildcardsInName(TableName),
                EscapePatternWildcardsInName(ColumnName),
                cancellationToken);
            OperationHandle = resp.OperationHandle;

            // Set _directResults so that dispose logic can check if operation was already closed
            _directResults = resp.DirectResults;

            // Common variables declared upfront
            TGetResultSetMetadataResp metadata;
            Schema schema;
            TRowSet rowSet;

            // For GetColumns, we need to enhance the result with BASE_TYPE_NAME
            if (Connection.AreResultsAvailableDirectly && resp.DirectResults?.ResultSet?.Results != null)
            {
                // Get data from direct results
                metadata = resp.DirectResults.ResultSetMetadata;
                schema = GetSchemaFromMetadata(metadata);
                rowSet = resp.DirectResults.ResultSet.Results;
            }
            else
            {
                // Poll and fetch results
                await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds, cancellationToken);

                // Get metadata
                metadata = await HiveServer2Connection.GetResultSetMetadataAsync(OperationHandle!, Connection.Client, cancellationToken);
                schema = GetSchemaFromMetadata(metadata);

                // Fetch the results
                rowSet = await Connection.FetchResultsAsync(OperationHandle!, BatchSize, cancellationToken);
            }

            // Common processing for both paths
            int columnCount = HiveServer2Reader.GetColumnCount(rowSet);
            int rowCount = HiveServer2Reader.GetRowCount(rowSet, columnCount);
            IReadOnlyList<IArrowArray> data = HiveServer2Reader.GetArrowArrayData(rowSet, columnCount, schema, Connection.DataTypeConversion);

            // Return the enhanced result with added BASE_TYPE_NAME column
            return EnhanceGetColumnsResult(schema, data, rowCount, metadata, rowSet);
        }

        private async Task<Schema> GetResultSetSchemaAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataResp response = await HiveServer2Connection.GetResultSetMetadataAsync(operationHandle, client, cancellationToken);
            return GetSchemaFromMetadata(response);
        }

        private async Task<QueryResult> GetQueryResult(TSparkDirectResults? directResults, CancellationToken cancellationToken)
        {
            // Set _directResults so that dispose logic can check if operation was already closed
            _directResults = directResults;

            Schema schema;
            if (Connection.AreResultsAvailableDirectly && directResults?.ResultSet?.Results != null)
            {
                TGetResultSetMetadataResp resultSetMetadata = directResults.ResultSetMetadata;
                schema = GetSchemaFromMetadata(resultSetMetadata);
                TRowSet rowSet = directResults.ResultSet.Results;
                int columnCount = HiveServer2Reader.GetColumnCount(rowSet);
                int rowCount = HiveServer2Reader.GetRowCount(rowSet, columnCount);
                IReadOnlyList<IArrowArray> data = HiveServer2Reader.GetArrowArrayData(rowSet, columnCount, schema, Connection.DataTypeConversion);

                return new QueryResult(rowCount, new HiveServer2Connection.HiveInfoArrowStream(schema, data));
            }

            await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds, cancellationToken);
            schema = await GetResultSetSchemaAsync(OperationHandle!, Connection.Client, cancellationToken);

            return new QueryResult(-1, Connection.NewReader(this, schema));
        }

        protected internal QueryResult EnhanceGetColumnsResult(Schema originalSchema, IReadOnlyList<IArrowArray> originalData,
            int rowCount, TGetResultSetMetadataResp metadata, TRowSet rowSet)
        {
            // Create a column map using Connection's GetColumnIndexMap method
            var columnMap = Connection.GetColumnIndexMap(metadata.Schema.Columns);

            // Get column indices - we know these columns always exist
            int typeNameIndex = columnMap["TYPE_NAME"];
            int dataTypeIndex = columnMap["DATA_TYPE"];
            int columnSizeIndex = columnMap["COLUMN_SIZE"];
            int decimalDigitsIndex = columnMap["DECIMAL_DIGITS"];

            // Extract the existing arrays
            StringArray typeNames = (StringArray)originalData[typeNameIndex];
            Int32Array originalColumnSizes = (Int32Array)originalData[columnSizeIndex];
            Int32Array originalDecimalDigits = (Int32Array)originalData[decimalDigitsIndex];

            // Create enhanced schema with BASE_TYPE_NAME column
            var enhancedFields = originalSchema.FieldsList.ToList();
            enhancedFields.Add(new Field("BASE_TYPE_NAME", StringType.Default, true));
            Schema enhancedSchema = new Schema(enhancedFields, originalSchema.Metadata);

            // Pre-allocate arrays to store our values
            int length = typeNames.Length;
            List<string> baseTypeNames = new List<string>(length);
            List<int> columnSizeValues = new List<int>(length);
            List<int> decimalDigitsValues = new List<int>(length);

            // Process each row
            for (int i = 0; i < length; i++)
            {
                string? typeName = typeNames.GetString(i);
                short colType = (short)rowSet.Columns[dataTypeIndex].I32Val.Values.Values[i];
                int columnSize = originalColumnSizes.GetValue(i).GetValueOrDefault();
                int decimalDigits = originalDecimalDigits.GetValue(i).GetValueOrDefault();

                // Create a TableInfo for this row
                var tableInfo = new HiveServer2Connection.TableInfo(string.Empty);

                // Process all types through SetPrecisionScaleAndTypeName
                Connection.SetPrecisionScaleAndTypeName(colType, typeName ?? string.Empty, tableInfo, columnSize, decimalDigits);

                // Get base type name
                string baseTypeName;
                if (tableInfo.BaseTypeName.Count > 0)
                {
                    string? baseTypeNameValue = tableInfo.BaseTypeName[0];
                    baseTypeName = baseTypeNameValue ?? string.Empty;
                }
                else
                {
                    baseTypeName = typeName ?? string.Empty;
                }
                baseTypeNames.Add(baseTypeName);

                // Get precision/scale values
                if (tableInfo.Precision.Count > 0)
                {
                    int? precisionValue = tableInfo.Precision[0];
                    columnSizeValues.Add(precisionValue.GetValueOrDefault(columnSize));
                }
                else
                {
                    columnSizeValues.Add(columnSize);
                }

                if (tableInfo.Scale.Count > 0)
                {
                    int? scaleValue = tableInfo.Scale[0];
                    decimalDigitsValues.Add(scaleValue.GetValueOrDefault(decimalDigits));
                }
                else
                {
                    decimalDigitsValues.Add(decimalDigits);
                }
            }

            // Create the Arrow arrays directly from our data arrays
            StringArray baseTypeNameArray = new StringArray.Builder().AppendRange(baseTypeNames).Build();
            Int32Array columnSizeArray = new Int32Array.Builder().AppendRange(columnSizeValues).Build();
            Int32Array decimalDigitsArray = new Int32Array.Builder().AppendRange(decimalDigitsValues).Build();

            // Create enhanced data with modified columns
            var enhancedData = new List<IArrowArray>(originalData);
            enhancedData[columnSizeIndex] = columnSizeArray;
            enhancedData[decimalDigitsIndex] = decimalDigitsArray;
            enhancedData.Add(baseTypeNameArray);

            return new QueryResult(rowCount, new HiveServer2Connection.HiveInfoArrowStream(enhancedSchema, enhancedData));
        }

        // Helper method to read all batches from a stream
        private async Task<(List<RecordBatch> Batches, Schema Schema, int TotalRows)> ReadAllBatchesAsync(
            IArrowArrayStream stream, CancellationToken cancellationToken)
        {
            List<RecordBatch> batches = new List<RecordBatch>();
            int totalRows = 0;
            Schema schema = stream.Schema;

            // Read all batches
            while (true)
            {
                var batch = await stream.ReadNextRecordBatchAsync(cancellationToken);
                if (batch == null) break;

                if (batch.Length > 0)
                {
                    batches.Add(batch);
                    totalRows += batch.Length;
                }
                else
                {
                    batch.Dispose();
                }
            }

            return (batches, schema, totalRows);
        }

        protected virtual async Task<QueryResult> GetColumnsExtendedAsync(CancellationToken cancellationToken = default)
        {
            // 1. Get all three results at once
            var columnsResult = await GetColumnsAsync(cancellationToken);
            if (columnsResult.Stream == null)
            {
                // TODO: Add log or throw
                return columnsResult;
            }

            var pkResult = await GetPrimaryKeysAsync(cancellationToken);

            // For FK lookup, we need to pass in the current catalog/schema/table as the foreign table
            var fkResult = await GetCrossReferenceAsForeignTableAsync(cancellationToken);

            try
            {
                // 2. Read all batches into memory
                List<RecordBatch> columnsBatches;
                int totalRows;
                Schema columnsSchema;
                StringArray? columnNames = null;
                int colNameIndex = -1;

                // Extract column data
                using (var stream = columnsResult.Stream)
                {
                    colNameIndex = stream.Schema.GetFieldIndex("COLUMN_NAME");
                    if (colNameIndex < 0)
                    {
                        // TODO: Add log or throw
                        return columnsResult; // Can't match without column names
                    }

                    var batchResult = await ReadAllBatchesAsync(stream, cancellationToken);
                    columnsBatches = batchResult.Batches;
                    columnsSchema = batchResult.Schema;
                    totalRows = batchResult.TotalRows;

                    if (columnsBatches.Count == 0)
                    {
                        // Return empty result with complete schema
                        return CreateEmptyExtendedColumnsResult(columnsSchema);
                    }

                    // Create column names array from all batches using ArrayDataConcatenator.Concatenate
                    List<ArrayData> columnNameArrayDataList = columnsBatches.Select(batch =>
                        batch.Column(colNameIndex).Data).ToList();
                    ArrayData? concatenatedColumnNames = ArrayDataConcatenator.Concatenate(columnNameArrayDataList);
                    columnNames = (StringArray)ArrowArrayFactory.BuildArray(concatenatedColumnNames!);
                }

                // 3. Create combined schema and prepare data
                var allFields = new List<Field>(columnsSchema.FieldsList);
                var combinedData = new List<IArrowArray>();

                // 4. Add all columns data by combining all batches
                for (int colIdx = 0; colIdx < columnsSchema.FieldsList.Count; colIdx++)
                {
                    if (columnsBatches.Count == 0)
                        continue;

                    var field = columnsSchema.GetFieldByIndex(colIdx);

                    // Collect arrays for this column from all batches
                    var columnArrays = new List<IArrowArray>();
                    foreach (var batch in columnsBatches)
                    {
                        columnArrays.Add(batch.Column(colIdx));
                    }

                    List<ArrayData> arrayDataList = columnArrays.Select(arr => arr.Data).ToList();
                    ArrayData? concatenatedData = ArrayDataConcatenator.Concatenate(arrayDataList);
                    IArrowArray array = ArrowArrayFactory.BuildArray(concatenatedData);
                    combinedData.Add(array);
                }

                // 5. Process PK and FK data using helper methods with selected fields
                await ProcessRelationshipDataSafe(pkResult, PrimaryKeyPrefix, "COLUMN_NAME",
                    PrimaryKeyFields, // Selected PK fields
                    columnNames, totalRows,
                    allFields, combinedData, cancellationToken);

                await ProcessRelationshipDataSafe(fkResult, ForeignKeyPrefix, "FKCOLUMN_NAME",
                    ForeignKeyFields, // Selected FK fields
                    columnNames, totalRows,
                    allFields, combinedData, cancellationToken);

                // 6. Return the combined result
                var combinedSchema = new Schema(allFields, columnsSchema.Metadata);

                return new QueryResult(totalRows, new HiveServer2Connection.HiveInfoArrowStream(combinedSchema, combinedData));
            }
            finally
            {
                // Dispose internal query results to ensure proper resource cleanup
                columnsResult.Stream?.Dispose();
                pkResult.Stream?.Dispose();
                fkResult.Stream?.Dispose();
            }
        }

        // Helper method to create an empty result with the complete extended columns schema
        protected static QueryResult CreateEmptyExtendedColumnsResult(Schema baseSchema)
        {
            // Create the complete schema with all fields
            var allFields = new List<Field>(baseSchema.FieldsList);
            // Add PK fields
            foreach (var field in PrimaryKeyFields)
            {
                allFields.Add(new Field(PrimaryKeyPrefix + field, StringType.Default, true));
            }
            // Add FK fields
            foreach (var field in ForeignKeyFields)
            {
                IArrowType fieldType = field != "KEQ_SEQ" ? StringType.Default : Int32Type.Default;
                allFields.Add(new Field(ForeignKeyPrefix + field, fieldType, true));
            }

            var combinedSchema = new Schema(allFields, baseSchema.Metadata);

            // Create empty arrays for all fields
            var combinedData = new List<IArrowArray>();
            foreach (var field in allFields)
            {
                switch (field.DataType.TypeId)
                {
                    case ArrowTypeId.String:
                        combinedData.Add(new StringArray.Builder().Build());
                        break;
                    case ArrowTypeId.Int8:
                        combinedData.Add(new Int8Array.Builder().Build());
                        break;
                    case ArrowTypeId.Int16:
                        combinedData.Add(new Int16Array.Builder().Build());
                        break;
                    case ArrowTypeId.Int32:
                        combinedData.Add(new Int32Array.Builder().Build());
                        break;
                    case ArrowTypeId.Int64:
                        combinedData.Add(new Int64Array.Builder().Build());
                        break;
                    case ArrowTypeId.Boolean:
                        combinedData.Add(new BooleanArray.Builder().Build());
                        break;
                    case ArrowTypeId.Float:
                        combinedData.Add(new FloatArray.Builder().Build());
                        break;
                    case ArrowTypeId.Double:
                        combinedData.Add(new  DoubleArray.Builder().Build());
                        break;
                    case ArrowTypeId.Date32:
                        combinedData.Add(new Date32Array.Builder().Build());
                        break;
                    case ArrowTypeId.Date64:
                        combinedData.Add(new Date64Array.Builder().Build());
                        break;
                    case ArrowTypeId.Timestamp:
                        combinedData.Add(new TimestampArray.Builder().Build());
                        break;
                    case ArrowTypeId.Decimal128:
                        combinedData.Add(new Decimal128Array.Builder((Decimal128Type)field.DataType).Build());
                        break;
                    default:
                        throw AdbcException.NotImplemented(
                            $"Data type '{field.DataType}' is not supported for empty extended columns result.");
                }
            }

            return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(combinedSchema, combinedData));
        }

        /**
         * Process relationship data (primary/foreign keys) from query results and add to the output.
         * This method handles data from PK/FK queries and correlates it with column data.
         *
         * How it works:
         * 1. Add relationship columns to the schema (PK/FK columns with prefixed names)
         * 2. Read relationship data from source records
         * 3. Build a mapping of column names to their relationship values
         * 4. Create arrays for each field, aligning values with the main column result
         */
        private async Task ProcessRelationshipDataSafe(QueryResult result, string prefix, string relationColNameField,
            string[] includeFields, StringArray colNames, int rowCount,
            List<Field> allFields, List<IArrowArray> combinedData, CancellationToken cancellationToken)
        {
            // STEP 1: Add relationship fields to the output schema
            // Each field name is prefixed (e.g., "PK_" for primary keys, "FK_" for foreign keys)
            if (result.Stream != null)
            {
                var schema = result.Stream.Schema;
                foreach (var fieldName in includeFields)
                {
                    int fieldIndex = schema.GetFieldIndex(fieldName);
                    IArrowType arrowType = StringType.Default; // fallback
                    if (fieldIndex >= 0)
                    {
                        arrowType = schema.GetFieldByIndex(fieldIndex).DataType;
                    }
                    allFields.Add(new Field(prefix + fieldName, arrowType, true));
                }
            }
            else
            {
                // fallback: if no stream, add as string
                foreach (var fieldName in includeFields)
                {
                    allFields.Add(new Field(prefix + fieldName, StringType.Default, true));
                }
            }

            // STEP 2: Create a dictionary to map column names to their relationship values
            // Structure: Dictionary<fieldName, Dictionary<columnName, relationshipValue>>
            // For primary keys - only columns that are PKs are stored:
            // {"COLUMN_NAME": {"id": "id"}}
            // For foreign keys - only columns that are FKs are stored:
            // {"FKCOLUMN_NAME": {"DOLocationId": "LocationId"}}
            var relationData = new Dictionary<string, Dictionary<string, object>>(StringComparer.OrdinalIgnoreCase);

            // STEP 3: Extract relationship data from the query result
            if (result.Stream != null)
            {
                using (var stream = result.Stream)
                {
                    // Find the column index that contains our key values (e.g., COLUMN_NAME for PK or FKCOLUMN_NAME for FK)
                    int keyColIndex = stream.Schema.GetFieldIndex(relationColNameField);
                    if (keyColIndex >= 0)
                    {
                        // STEP 3.1: Process each record batch from the relationship data source
                        while (true)
                        {
                            var batch = await stream.ReadNextRecordBatchAsync(cancellationToken);
                            if (batch == null) break;

                            // STEP 3.2: Map field names to their column indices for quick lookup
                            Dictionary<string, int> fieldIndices = new Dictionary<string, int>();
                            foreach (var fieldName in includeFields)
                            {
                                int index = stream.Schema.GetFieldIndex(fieldName);
                                if (index >= 0) fieldIndices[fieldName] = index;
                            }

                            // STEP 3.3: Process each row in the batch
                            for (int i = 0; i < batch.Length; i++)
                            {
                                // Get the key column value (e.g., column name this relationship applies to)
                                StringArray keyCol = (StringArray)batch.Column(keyColIndex);
                                if (keyCol.IsNull(i)) continue;

                                string keyValue = keyCol.GetString(i);

                                if (string.IsNullOrEmpty(keyValue)) continue;

                                // STEP 3.4: For each included field, extract its value and store in our map
                                foreach (var pair in fieldIndices)
                                {
                                    // Ensure we have an entry for this field
                                    if (!relationData.TryGetValue(pair.Key, out var fieldData))
                                    {
                                        fieldData = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);
                                        relationData[pair.Key] = fieldData;
                                    }
                                    // Store the relationship value: columnName -> value
                                    IArrowArray fieldArray = batch.Column(pair.Value);
                                    if (fieldArray is Int32Array int32ArrayField)
                                    {
                                        var val = int32ArrayField.GetValue(i);
                                        relationData[pair.Key][keyValue] = val.GetValueOrDefault();
                                    }
                                    else
                                    {
                                        // Default: treat as string array
                                        var stringArrayFallback = (StringArray)fieldArray;
                                        relationData[pair.Key][keyValue] = stringArrayFallback.GetString(i);
                                    }

                                }
                            }
                        }
                    }
                }
            }

            // STEP 4: Build Arrow arrays for each relationship field
            foreach (var fieldName in includeFields)
            {
                var fieldData = relationData.ContainsKey(fieldName) ? relationData[fieldName] : null;
                IArrowType arrowType = StringType.Default;
                if (result.Stream != null)
                {
                    int fieldIndex = result.Stream.Schema.GetFieldIndex(fieldName);
                    if (fieldIndex >= 0)
                        arrowType = result.Stream.Schema.GetFieldByIndex(fieldIndex).DataType;
                }

                if (arrowType.TypeId == ArrowTypeId.Int32)
                {
                    var builder = new Int32Array.Builder();
                    for (int i = 0; i < colNames.Length; i++)
                    {
                        string? colName = colNames.GetString(i);
                        if (!string.IsNullOrEmpty(colName) && fieldData != null && fieldData.TryGetValue(colName!, out var fieldValue))
                        {
                            if (fieldValue is int intVal)
                            {
                                builder.Append(intVal);
                            }
                            else if (fieldValue is string strVal && int.TryParse(strVal, out int parsed))
                            {
                                builder.Append(parsed);
                            }
                            else
                            {
                                builder.AppendNull();
                            }
                        }
                        else
                        {
                            builder.AppendNull();
                        }
                    }
                    combinedData.Add(builder.Build());
                }
                else
                {
                    var builder = new StringArray.Builder();
                    for (int i = 0; i < colNames.Length; i++)
                    {
                        string? colName = colNames.GetString(i);
                        string? value = null;
                        if (!string.IsNullOrEmpty(colName) &&
                            fieldData != null &&
                            fieldData.TryGetValue(colName!, out var fieldValue))
                        {
                            value = (string?)fieldValue;
                        }
                        builder.Append(value);
                    }
                    combinedData.Add(builder.Build());
                }
            }
        }

        /// <summary>
        /// Clears the current query result and disposes of it if necessary.
        /// </summary>
        private void ClearCurrentQueryResult()
        {
            _currentQueryResult?.Stream?.Dispose();
            _currentQueryResult = null;
        }
    }
}
