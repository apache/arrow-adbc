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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public abstract class HiveServer2Connection : AdbcConnection
    {
        internal const long BatchSizeDefault = 50000;
        internal const int PollTimeMillisecondsDefault = 500;
        private const string userAgent = "AdbcExperimental/0.0";

        protected TOperationHandle? operationHandle;
        internal TTransport? transport;
        private TCLIService.Client? client;
        internal TSessionHandle? sessionHandle;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            this.Properties = properties;
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

        protected internal string VendorVersion => _vendorVersion.Value;

        protected string VendorName => _vendorName.Value;

        internal IReadOnlyDictionary<string, string> Properties { get; }

        protected internal TProtocolVersion ProtocolVersion { get; private set; }

        protected abstract IReadOnlyList<TProtocolVersion> SupportedProtocolVersions { get; }

        internal async Task OpenAsync()
        {
            TProtocol protocol = await CreateProtocolAsync();
            this.transport = protocol.Transport;
            this.client = new TCLIService.Client(protocol);
            TOpenSessionResp? s0 = null;
            Exception? lastException = null;

            if (SupportedProtocolVersions.Count < 1)
                throw new InvalidOperationException("Invalid driver implementation. Must contain at least one supported Thrift protocol version.");

            // Try each protocol version, until a successful connection is made.
            // All other exception should be not be caught (i.e., no response, no host, rejected, authentication, etc.
            foreach (TProtocolVersion protocolVersion in SupportedProtocolVersions)
            {
                s0 = null;
                try
                {
                    s0 = await this.client.OpenSession(CreateSessionRequest(protocolVersion));
                    ProtocolVersion = protocolVersion;
                    break;
                }
                catch (TApplicationException ex) when (ex.Type == TApplicationException.ExceptionType.ProtocolError)
                {
                    lastException = ex;
                    continue;
                }
            }

            // If we still don't have a connection after trying all the protocols, raise the last known exception.
            if (s0 == null) throw lastException!;

            this.sessionHandle = s0.SessionHandle;
        }

        protected abstract ValueTask<TProtocol> CreateProtocolAsync();

        protected abstract TOpenSessionReq CreateSessionRequest(TProtocolVersion protocolVersion);

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new NotImplementedException();
        }

        static internal async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds)
        {
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { await Task.Delay(pollTimeMilliseconds); }
                TGetOperationStatusReq request = new(operationHandle);
                statusResponse = await client.GetOperationStatus(request);
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

        protected internal bool IsHiveServer2Protocol => GetIsHiveServer2Protocol(ProtocolVersion);

        protected internal bool IsSparkProtocol => GetIsSparkProtocol(ProtocolVersion);

        internal static bool GetIsHiveServer2Protocol(TProtocolVersion protocolVersion) =>
            protocolVersion is >= TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1 and <= TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11;

        internal static bool GetIsSparkProtocol(TProtocolVersion protocolVersion) =>
            protocolVersion is >= TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1 and <= TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7;

        internal static async Task<Schema> GetResultSetSchemaAsync(TOperationHandle operationHandle, TCLIService.IAsync client, TProtocolVersion protocolVersion, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataResp response = await GetResultSetMetadataAsync(operationHandle, client, cancellationToken);
            return SchemaParser.GetArrowSchema(response.Schema, protocolVersion);
        }

        internal static async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataReq request = new(operationHandle);
            TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
            return response;
        }

        internal static async Task<TFetchResultsResp> FetchNextAsync(TOperationHandle operationHandle, TCLIService.IAsync client, long batchSize, CancellationToken cancellationToken = default)
        {
            TFetchResultsReq request = new(operationHandle, TFetchOrientation.FETCH_NEXT, batchSize);
            TFetchResultsResp response = await client.FetchResults(request, cancellationToken);
            return response;
        }
    }
}
