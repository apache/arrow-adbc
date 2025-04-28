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
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2Statement : AdbcStatement
    {
        private const string GetPrimaryKeysCommandName = "getprimarykeys";
        private const string GetCrossReferenceCommandName = "getcrossreference";
        private const string GetCatalogsCommandName = "getcatalogs";
        private const string GetSchemasCommandName = "getschemas";
        private const string GetTablesCommandName = "gettables";
        private const string GetColumnsCommandName = "getcolumns";
        private const string SupportedMetadataCommands =
            GetCatalogsCommandName + "," +
            GetSchemasCommandName + "," +
            GetTablesCommandName + "," +
            GetColumnsCommandName + "," +
            GetPrimaryKeysCommandName + "," +
            GetCrossReferenceCommandName;

        internal HiveServer2Statement(HiveServer2Connection connection)
        {
            Connection = connection;
            ValidateOptions(connection.Properties);
        }

        protected virtual void SetStatementProperties(TExecuteStatementReq statement)
        {
            statement.QueryTimeout = QueryTimeoutSeconds;
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
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ex.Message}'", ex);
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
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ex.Message}'", ex);
            }
        }

        private async Task<QueryResult> ExecuteQueryAsyncInternal(CancellationToken cancellationToken = default)
        {
            if (IsMetadataCommand)
            {
                return await ExecuteMetadataCommandQuery(cancellationToken);
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
            // Store metadata for use in readers
            Schema schema = Connection.SchemaParser.GetArrowSchema(metadata.Schema, Connection.DataTypeConversion);
            return new QueryResult(-1, Connection.NewReader(this, schema, metadata));
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
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ex.Message}'", ex);
            }
        }

        public async Task<UpdateResult> ExecuteUpdateAsyncInternal(CancellationToken cancellationToken = default)
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
            if (affectedRowsField == null) return new UpdateResult(-1);

            long? affectedRows = null;
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
            return new UpdateResult(affectedRows ?? -1);
        }

        public override async Task<UpdateResult> ExecuteUpdateAsync()
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
                throw new HiveServer2Exception($"An unexpected error occurred while fetching results. '{ex.Message}'", ex);
            }
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
                default:
                    throw AdbcException.NotImplemented($"Option '{key}' is not implemented.");
            }
        }

        protected async Task ExecuteStatementAsync(CancellationToken cancellationToken = default)
        {
            if (Connection.SessionHandle == null)
            {
                throw new InvalidOperationException("Invalid session");
            }

            TExecuteStatementReq executeRequest = new TExecuteStatementReq(Connection.SessionHandle, SqlQuery!);
            SetStatementProperties(executeRequest);
            TExecuteStatementResp executeResponse = await Connection.Client.ExecuteStatement(executeRequest, cancellationToken);
            if (executeResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(executeResponse.Status.ErrorMessage)
                    .SetSqlState(executeResponse.Status.SqlState)
                    .SetNativeError(executeResponse.Status.ErrorCode);
            }
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
        protected internal TSparkDirectResults? _directResults { get; set; }

        public HiveServer2Connection Connection { get; private set; }

        public TOperationHandle? OperationHandle { get; private set; }

        // Keep the original Client property for internal use
        public TCLIService.Client Client => Connection.Client;

        private void UpdatePollTimeIfValid(string key, string value) => PollTimeMilliseconds = !string.IsNullOrEmpty(key) && int.TryParse(value, result: out int pollTimeMilliseconds) && pollTimeMilliseconds >= 0
            ? pollTimeMilliseconds
            : throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than or equal to 0.");

        private void UpdateBatchSizeIfValid(string key, string value) => BatchSize = !string.IsNullOrEmpty(value) && long.TryParse(value, out long batchSize) && batchSize > 0
            ? batchSize
            : throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than zero.");

        public override void Dispose()
        {
            if (OperationHandle != null)
            {
                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                TCloseOperationReq request = new TCloseOperationReq(OperationHandle);
                Connection.Client.CloseOperation(request, cancellationToken).Wait();
                OperationHandle = null;
            }

            base.Dispose();
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
                null or "" => throw new ArgumentNullException(nameof(SqlQuery), $"Metadata command for property 'SqlQuery' must not be empty or null. Supported metadata commands: {SupportedMetadataCommands}"),
                _ => throw new NotSupportedException($"Metadata command '{SqlQuery}' is not supported. Supported metadata commands: {SupportedMetadataCommands}"),
            };
        }

        private async Task<QueryResult> GetCrossReferenceAsync(CancellationToken cancellationToken = default)
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

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        private async Task<QueryResult> GetPrimaryKeysAsync(CancellationToken cancellationToken = default)
        {
            TGetPrimaryKeysResp resp = await Connection.GetPrimaryKeysAsync(
                CatalogName,
                SchemaName,
                TableName,
                cancellationToken);
            OperationHandle = resp.OperationHandle;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        private async Task<QueryResult> GetCatalogsAsync(CancellationToken cancellationToken = default)
        {
            TGetCatalogsResp resp = await Connection.GetCatalogsAsync(cancellationToken);
            OperationHandle = resp.OperationHandle;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        private async Task<QueryResult> GetSchemasAsync(CancellationToken cancellationToken = default)
        {
            TGetSchemasResp resp = await Connection.GetSchemasAsync(
                CatalogName,
                SchemaName,
                cancellationToken);
            OperationHandle = resp.OperationHandle;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        private async Task<QueryResult> GetTablesAsync(CancellationToken cancellationToken = default)
        {
            List<string>? tableTypesList = this.TableTypes?.Split(',').ToList();
            TGetTablesResp resp = await Connection.GetTablesAsync(
                CatalogName,
                SchemaName,
                TableName,
                tableTypesList,
                cancellationToken);
            OperationHandle = resp.OperationHandle;

            return await GetQueryResult(resp.DirectResults, cancellationToken);
        }

        private async Task<QueryResult> GetColumnsAsync(CancellationToken cancellationToken = default)
        {
            TGetColumnsResp resp = await Connection.GetColumnsAsync(
                CatalogName,
                SchemaName,
                TableName,
                ColumnName,
                cancellationToken);
            OperationHandle = resp.OperationHandle;

            // Common variables declared upfront
            TGetResultSetMetadataResp metadata;
            Schema schema;
            TRowSet rowSet;

            // For GetColumns, we need to enhance the result with BASE_TYPE_NAME
            if (Connection.AreResultsAvailableDirectly && resp.DirectResults?.ResultSet?.Results != null)
            {
                // Get data from direct results
                metadata = resp.DirectResults.ResultSetMetadata;
                schema = Connection.SchemaParser.GetArrowSchema(metadata.Schema, Connection.DataTypeConversion);
                rowSet = resp.DirectResults.ResultSet.Results;
            }
            else
            {
                // Poll and fetch results
                await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds, cancellationToken);

                // Get metadata
                metadata = await HiveServer2Connection.GetResultSetMetadataAsync(OperationHandle!, Connection.Client, cancellationToken);
                schema = Connection.SchemaParser.GetArrowSchema(metadata.Schema, Connection.DataTypeConversion);

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
            return Connection.SchemaParser.GetArrowSchema(response.Schema, Connection.DataTypeConversion);
        }

        private async Task<QueryResult> GetQueryResult(TSparkDirectResults? directResults, CancellationToken cancellationToken)
        {
            Schema schema;
            if (Connection.AreResultsAvailableDirectly && directResults?.ResultSet?.Results != null)
            {
                TGetResultSetMetadataResp resultSetMetadata = directResults.ResultSetMetadata;
                schema = Connection.SchemaParser.GetArrowSchema(resultSetMetadata.Schema, Connection.DataTypeConversion);
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
    }
}
