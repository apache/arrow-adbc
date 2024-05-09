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
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public abstract class HiveServer2Statement : AdbcStatement
    {
        private const int PollTimeMillisecondsDefault = 500;
        private const int BatchSizeDefault = 50000;
        protected internal HiveServer2Connection connection;
        protected internal TOperationHandle? operationHandle;

        protected HiveServer2Statement(HiveServer2Connection connection)
        {
            this.connection = connection;
        }

        protected virtual void SetStatementProperties(TExecuteStatementReq statement)
        {
        }

        protected abstract IArrowArrayStream NewReader<T>(T statement, Schema schema) where T : HiveServer2Statement;

        public override QueryResult ExecuteQuery() => ExecuteQueryAsync().AsTask().Result;

        public override UpdateResult ExecuteUpdate() => ExecuteUpdateAsync().Result;

        public override async ValueTask<QueryResult> ExecuteQueryAsync()
        {
            await ExecuteStatementAsync();
            await PollForResponseAsync();
            Schema schema = await GetSchemaAsync();

            // TODO: Ensure this is set dynamically based on server capabilities
            return new QueryResult(-1, NewReader(this, schema));
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

            // If no altered rows, i.e. DDC statements, then -1 is the default.
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

            return new UpdateResult(affectedRows ?? -1);
        }

        protected async Task ExecuteStatementAsync()
        {
            TExecuteStatementReq executeRequest = new TExecuteStatementReq(this.connection.sessionHandle, this.SqlQuery);
            SetStatementProperties(executeRequest);
            TExecuteStatementResp executeResponse = await this.connection.Client.ExecuteStatement(executeRequest);
            if (executeResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(executeResponse.Status.ErrorMessage)
                    .SetSqlState(executeResponse.Status.SqlState)
                    .SetNativeError(executeResponse.Status.ErrorCode);
            }
            this.operationHandle = executeResponse.OperationHandle;
        }

        protected async Task PollForResponseAsync()
        {
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { await Task.Delay(PollTimeMilliseconds); }
                TGetOperationStatusReq request = new TGetOperationStatusReq(this.operationHandle);
                statusResponse = await this.connection.Client.GetOperationStatus(request);
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
        }

        protected async ValueTask<Schema> GetSchemaAsync()
        {
            TGetResultSetMetadataReq request = new TGetResultSetMetadataReq(this.operationHandle);
            TGetResultSetMetadataResp response = await this.connection.Client.GetResultSetMetadata(request);
            return SchemaParser.GetArrowSchema(response.Schema);
        }

        protected internal int PollTimeMilliseconds { get; } = PollTimeMillisecondsDefault;

        protected internal int BatchSize { get; } = BatchSizeDefault;

        public override void Dispose()
        {
            if (this.operationHandle != null)
            {
                TCloseOperationReq request = new TCloseOperationReq(this.operationHandle);
                this.connection.Client.CloseOperation(request).Wait();
                this.operationHandle = null;
            }

            base.Dispose();
        }
    }
}
