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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public abstract class HiveServer2Connection : AdbcConnection
    {
        const string userAgent = "AdbcExperimental/0.0";

        protected TOperationHandle? operationHandle;
        protected readonly IReadOnlyDictionary<string, string> properties;
        internal TTransport? transport;
        internal TCLIService.Client? client;
        internal TSessionHandle? sessionHandle;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            this.properties = properties;
            // Note: "LazyThreadSafetyMode.PublicationOnly" is thread-safe initialization where
            // the first successful thread sets the value. If an exception is thrown, initialization
            // will retry until it successfully returns a value without an exception.
            // https://learn.microsoft.com/en-us/dotnet/framework/performance/lazy-initialization#exceptions-in-lazy-objects
            _vendorVersion = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_VER), LazyThreadSafetyMode.PublicationOnly);
            _vendorName = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_NAME), LazyThreadSafetyMode.PublicationOnly);
        }

        internal TCLIService.Client Client
        {
            get { return this.client ?? throw new InvalidOperationException("connection not open"); }
        }

        protected string VendorVersion => _vendorVersion.Value;

        protected string VendorName => _vendorName.Value;

        internal async Task OpenAsync()
        {
            TProtocol protocol = await CreateProtocolAsync();
            this.transport = protocol.Transport;
            this.client = new TCLIService.Client(protocol);

            var s0 = await this.client.OpenSession(CreateSessionRequest());
            this.sessionHandle = s0.SessionHandle;
        }

        protected abstract ValueTask<TProtocol> CreateProtocolAsync();

        protected abstract TOpenSessionReq CreateSessionRequest();

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new NotImplementedException();
        }

        protected void PollForResponse()
        {
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { Thread.Sleep(500); }
                TGetOperationStatusReq request = new TGetOperationStatusReq(this.operationHandle);
                statusResponse = this.Client.GetOperationStatus(request).Result;
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
        }

        private string GetInfoTypeStringValue(TGetInfoType infoType)
        {
            TGetInfoReq req = new()
            {
                SessionHandle = this.sessionHandle ?? throw new InvalidOperationException("session not created"),
                InfoType = infoType,
            };

            TGetInfoResp getInfoResp = Client.GetInfo(req).Result;
            if (getInfoResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(getInfoResp.Status.ErrorMessage)
                    .SetNativeError(getInfoResp.Status.ErrorCode)
                    .SetSqlState(getInfoResp.Status.SqlState);
            }

            return getInfoResp.InfoValue.StringValue;
        }

        public override void Dispose()
        {
            if (this.client != null)
            {
                TCloseSessionReq r6 = new TCloseSessionReq(this.sessionHandle);
                this.client.CloseSession(r6).Wait();

                this.transport?.Close();
                this.client.Dispose();
                this.transport = null;
                this.client = null;
            }
        }

        protected Schema GetSchema()
        {
            TGetResultSetMetadataReq request = new TGetResultSetMetadataReq(this.operationHandle);
            TGetResultSetMetadataResp response = this.Client.GetResultSetMetadata(request).Result;
            return SchemaParser.GetArrowSchema(response.Schema);
        }
    }
}
