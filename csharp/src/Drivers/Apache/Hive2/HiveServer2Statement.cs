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

using System.Threading;
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

        protected void ExecuteStatement()
        {
            TExecuteStatementReq executeRequest = new TExecuteStatementReq(this.connection.sessionHandle, this.SqlQuery);
            SetStatementProperties(executeRequest);
            var executeResponse = this.connection.Client.ExecuteStatement(executeRequest).Result;
            if (executeResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(executeResponse.Status.ErrorMessage)
                    .SetSqlState(executeResponse.Status.SqlState)
                    .SetNativeError(executeResponse.Status.ErrorCode);
            }
            this.operationHandle = executeResponse.OperationHandle;
        }

        protected void PollForResponse()
        {
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { Thread.Sleep(500); }
                TGetOperationStatusReq request = new TGetOperationStatusReq(this.operationHandle);
                statusResponse = this.connection.Client.GetOperationStatus(request).Result;
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
        }

        protected Schema GetSchema()
        {
            TGetResultSetMetadataReq request = new TGetResultSetMetadataReq(this.operationHandle);
            TGetResultSetMetadataResp response = this.connection.Client.GetResultSetMetadata(request).Result;
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
