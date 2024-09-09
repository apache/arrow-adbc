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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal abstract class HiveServer2Statement : AdbcStatement
    {
        protected HiveServer2Statement(HiveServer2Connection connection)
        {
            Connection = connection;
        }

        protected virtual void SetStatementProperties(TExecuteStatementReq statement)
        {
        }

        public override QueryResult ExecuteQuery() => ExecuteQueryAsync().AsTask().Result;

        public override UpdateResult ExecuteUpdate() => ExecuteUpdateAsync().Result;

        public override async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            await ExecuteStatementAsync();
            await HiveServer2Connection.PollForResponseAsync(OperationHandle!, Connection.Client, PollTimeMilliseconds);
            Schema schema = await GetResultSetSchemaAsync(OperationHandle!, Connection.Client);

            // TODO: Ensure this is set dynamically based on server capabilities
            return new QueryResult(-1, Connection.NewReader(this, schema));
        }

        private async Task<Schema> GetResultSetSchemaAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataResp response = await HiveServer2Connection.GetResultSetMetadataAsync(operationHandle, client, cancellationToken);
            return Connection.SchemaParser.GetArrowSchema(response.Schema, Connection.DataTypeConversion);
        }

        public override async Task<UpdateResult> ExecuteUpdateAsync()
        {
            const string NumberOfAffectedRowsColumnName = "num_affected_rows";

            QueryResult queryResult = await ExecuteQueryAsync();
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
                using RecordBatch nextBatch = await stream.ReadNextRecordBatchAsync();
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

        public override void SetOption(string key, string value)
        {
            switch (key)
            {
                case Options.PollTimeMilliseconds:
                    UpdatePollTimeIfValid(key, value);
                    break;
                case Options.BatchSize:
                    UpdateBatchSizeIfValid(key, value);
                    break;
                default:
                    throw AdbcException.NotImplemented($"Option '{key}' is not implemented.");
            }
        }

        protected async Task ExecuteStatementAsync()
        {
            TExecuteStatementReq executeRequest = new TExecuteStatementReq(Connection.SessionHandle, SqlQuery);
            SetStatementProperties(executeRequest);
            TExecuteStatementResp executeResponse = await Connection.Client.ExecuteStatement(executeRequest);
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

        public HiveServer2Connection Connection { get; private set; }

        public TOperationHandle? OperationHandle { get; private set; }

        /// <summary>
        /// Provides the constant string key values to the <see cref="AdbcStatement.SetOption(string, string)" /> method.
        /// </summary>
        public class Options
        {
            // Options common to all HiveServer2Statement-derived drivers go here
            public const string PollTimeMilliseconds = "adbc.statement.polltime_milliseconds";
            public const string BatchSize = "adbc.statement.batch_size";
        }

        private void UpdatePollTimeIfValid(string key, string value) => PollTimeMilliseconds = !string.IsNullOrEmpty(key) && int.TryParse(value, result: out int pollTimeMilliseconds) && pollTimeMilliseconds >= 0
            ? pollTimeMilliseconds
            : throw new ArgumentException($"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than or equal to zero.", nameof(value));

        private void UpdateBatchSizeIfValid(string key, string value) => BatchSize = !string.IsNullOrEmpty(value) && int.TryParse(value, out int batchSize) && batchSize > 0
            ? batchSize
            : throw new ArgumentException($"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than zero.", nameof(value));

        public override void Dispose()
        {
            if (OperationHandle != null)
            {
                TCloseOperationReq request = new TCloseOperationReq(OperationHandle);
                Connection.Client.CloseOperation(request).Wait();
                OperationHandle = null;
            }

            base.Dispose();
        }
    }
}
