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

using System.Threading.Tasks;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public abstract class HiveServer2Statement : AdbcStatement
    {
        protected HiveServer2Connection connection;
        protected TOperationHandle? operationHandle;

        protected HiveServer2Statement(HiveServer2Connection connection)
        {
            this.connection = connection;
        }

        protected virtual void SetStatementProperties(TExecuteStatementReq statement)
        {
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
                if (statusResponse != null) { await Task.Delay(500); }
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
