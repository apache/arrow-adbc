﻿/*
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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2Statement : AdbcStatement
    {
        private const string GetPrimayKeysCommandName = "getprimarykeys";
        private const string GetCrossReferenceCommandName = "getcrossreference";

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
                QueryResult result = SqlQuery?.ToLowerInvariant() switch
                {
                    GetPrimayKeysCommandName => await GetPrimaryKeysAsync(cancellationToken),
                    GetCrossReferenceCommandName => await GetCrossReferenceAsync(cancellationToken),
                    "" => throw new ArgumentNullException(nameof(SqlQuery)),
                    null => throw new ArgumentNullException(nameof(SqlQuery)),
                    _ => throw new NotSupportedException($"Metadata command '{SqlQuery}' is not supported."),
                };
                return result;
            }
            else
            {
                // this could either:
                // take QueryTimeoutSeconds * 3
                // OR
                // take QueryTimeoutSeconds (but this could be restricting)
                await ExecuteStatementAsync(cancellationToken); // --> get QueryTimeout +
                await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds, cancellationToken); // + poll, up to QueryTimeout
                Schema schema = await GetResultSetSchemaAsync(OperationHandle!, Connection.Client, cancellationToken); // + get the result, up to QueryTimeout

                // Store metadata for use in readers
                return new QueryResult(-1, Connection.NewReader(this, schema));
            }
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
        protected internal string? ForeignCatalogName { get; set; }
        protected internal string? ForeignSchemaName { get; set; }
        protected internal string? ForeignTableName { get; set; }

        public HiveServer2Connection Connection { get; private set; }

        public TOperationHandle? OperationHandle { get; private set; }

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
            await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds, cancellationToken); // + poll, up to QueryTimeout
            Schema schema = await GetResultSetSchemaAsync(OperationHandle!, Connection.Client, cancellationToken); // + get the result, up to QueryTimeout

            return new QueryResult(-1, Connection.NewReader(this, schema));
        }

        private async Task<QueryResult> GetPrimaryKeysAsync(CancellationToken cancellationToken = default)
        {
            // Note: allows catalog/schema/table to be null or empty
            TGetPrimaryKeysResp resp = await Connection.GetPrimaryKeysAsync(
                CatalogName,
                SchemaName,
                TableName,
                cancellationToken);

            OperationHandle = resp.OperationHandle;
            await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds, cancellationToken); // + poll, up to QueryTimeout
            Schema schema = await GetResultSetSchemaAsync(OperationHandle!, Connection.Client, cancellationToken); // + get the result, up to QueryTimeout

            return new QueryResult(-1, Connection.NewReader(this, schema));
        }

        private async Task<Schema> GetResultSetSchemaAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataResp response = await HiveServer2Connection.GetResultSetMetadataAsync(operationHandle, client, cancellationToken);
            return Connection.SchemaParser.GetArrowSchema(response.Schema, Connection.DataTypeConversion);
        }
    }
}
