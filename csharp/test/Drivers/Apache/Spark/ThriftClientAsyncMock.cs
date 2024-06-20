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
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    internal class ThriftClientAsyncMock : MockServerBase<TCLIService.IAsync>
    {
        private ThriftClientAsyncMock(TCLIService.IAsync proxy) : base(proxy)
        {
        }

        internal static MockServerBase<TCLIService.IAsync> NewInstance()
        {
            var result = new ThriftClientAsyncMock(new ThriftClientAsyncProxy());

            return result;
        }

        internal class ThriftClientAsyncProxy : TCLIService.IAsync
        {
            public Task<TCancelDelegationTokenResp> CancelDelegationToken(TCancelDelegationTokenReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TCancelOperationResp> CancelOperation(TCancelOperationReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TCloseOperationResp> CloseOperation(TCloseOperationReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TCloseSessionResp> CloseSession(TCloseSessionReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TDownloadDataResp> DownloadData(TDownloadDataReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TExecuteStatementResp> ExecuteStatement(TExecuteStatementReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TFetchResultsResp> FetchResults(TFetchResultsReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetCatalogsResp> GetCatalogs(TGetCatalogsReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetColumnsResp> GetColumns(TGetColumnsReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetCrossReferenceResp> GetCrossReference(TGetCrossReferenceReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetDelegationTokenResp> GetDelegationToken(TGetDelegationTokenReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetFunctionsResp> GetFunctions(TGetFunctionsReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetInfoResp> GetInfo(TGetInfoReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetOperationStatusResp> GetOperationStatus(TGetOperationStatusReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetPrimaryKeysResp> GetPrimaryKeys(TGetPrimaryKeysReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetQueryIdResp> GetQueryId(TGetQueryIdReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetResultSetMetadataResp> GetResultSetMetadata(TGetResultSetMetadataReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetSchemasResp> GetSchemas(TGetSchemasReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetTablesResp> GetTables(TGetTablesReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TGetTableTypesResp> GetTableTypes(TGetTableTypesReq req, CancellationToken cancellationToken = default)
            {
                TStatus status = new(TStatusCode.SUCCESS_STATUS);
                StringArray.Builder stringBuilder = new();
                stringBuilder.Append("TABLE");
                stringBuilder.Append("VIEW");
                TTypeDesc stringTypeDesc = new()
                {
                    Types = [ new TTypeEntry() { PrimitiveEntry = new TPrimitiveTypeEntry(TTypeId.STRING_TYPE) } ]
                };
                List<TColumn> columns = [new TColumn() { StringVal = new TStringColumn(stringBuilder.Build()) }];
                TRowSet rowSet = new TRowSet()
                {
                    ColumnCount = 1,
                    Columns = columns,
                };
                List<TColumnDesc> columnDesc = [new TColumnDesc("table_type", stringTypeDesc, 0)];
                TTableSchema tableSchema = new()
                {
                    Columns = columnDesc,
                };
                TGetResultSetMetadataResp resultSetMetadataResp = new(status)
                {
                    Schema = tableSchema,
                };
                TFetchResultsResp resultSet = new(status)
                {
                    ResultSetMetadata = resultSetMetadataResp,
                    Results = rowSet,
                };
                TSparkDirectResults directResults = new()
                {
                    ResultSet = resultSet,
                };
                TGetTableTypesResp resp = new(status)
                {
                    DirectResults = directResults,
                };

                return Task.FromResult(resp);
            }

            public Task<TGetTypeInfoResp> GetTypeInfo(TGetTypeInfoReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TOpenSessionResp> OpenSession(TOpenSessionReq req, CancellationToken cancellationToken = default)
            {
                TOpenSessionResp resp = new()
                {
                    Status = new TStatus(TStatusCode.SUCCESS_STATUS),
                    ServerProtocolVersion = req.Client_protocol,
                    SessionHandle = new TSessionHandle(new THandleIdentifier(Guid.NewGuid().ToByteArray(), Guid.NewGuid().ToByteArray())),
                };
                return Task.FromResult(resp);
            }

            public Task<TRenewDelegationTokenResp> RenewDelegationToken(TRenewDelegationTokenReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TSetClientInfoResp> SetClientInfo(TSetClientInfoReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
            public Task<TUploadDataResp> UploadData(TUploadDataReq req, CancellationToken cancellationToken = default) => throw new NotImplementedException();
        }
    }
}
