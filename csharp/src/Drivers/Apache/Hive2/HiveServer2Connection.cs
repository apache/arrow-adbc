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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal abstract class HiveServer2Connection : AdbcConnection
    {
        private bool _disposed;
        internal const long BatchSizeDefault = 50000;
        internal const int PollTimeMillisecondsDefault = 500;

        private TTransport? _transport;
        private TCLIService.Client? _client;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;

        internal HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            Properties = properties;

            // Note: "LazyThreadSafetyMode.PublicationOnly" is thread-safe initialization where
            // the first successful thread sets the value. If an exception is thrown, initialization
            // will retry until it successfully returns a value without an exception.
            // https://learn.microsoft.com/en-us/dotnet/framework/performance/lazy-initialization#exceptions-in-lazy-objects
            _vendorVersion = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_VER), LazyThreadSafetyMode.PublicationOnly);
            _vendorName = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_NAME), LazyThreadSafetyMode.PublicationOnly);
        }

        internal TCLIService.Client Client
        {
            get { return _client ?? throw new InvalidOperationException("connection not open"); }
        }

        internal string VendorVersion => _vendorVersion.Value;

        internal string VendorName => _vendorName.Value;

        internal IReadOnlyDictionary<string, string> Properties { get; }

        internal async Task OpenAsync()
        {
            await TraceAsync(async (activity) =>
            {
                TTransport transport = await CreateTransportAsync();
                TProtocol protocol = await CreateProtocolAsync(transport);
                _transport = protocol.Transport;
                _client = new TCLIService.Client(protocol);
                TOpenSessionReq request = CreateSessionRequest();

                activity?.AddEvent(new ActivityEvent("openSession.start"));
                TOpenSessionResp? session = await Client.OpenSession(request);
                activity?.AddEvent(new ActivityEvent("openSession.end"));

                // Some responses don't raise an exception. Explicitly check the status.
                if (session == null)
                {
                    throw new HiveServer2Exception("unable to open session. unknown error.");
                }
                else if (session.Status.StatusCode != TStatusCode.SUCCESS_STATUS)
                {
                    throw new HiveServer2Exception(session.Status.ErrorMessage)
                        .SetNativeError(session.Status.ErrorCode)
                        .SetSqlState(session.Status.SqlState);
                }
                SessionHandle = session.SessionHandle;
            });
        }

        internal TSessionHandle? SessionHandle { get; private set; }

        protected internal DataTypeConversion DataTypeConversion { get; set; } = DataTypeConversion.None;

        protected internal HiveServer2TlsOption TlsOptions { get; set; } = HiveServer2TlsOption.Empty;

        protected internal int HttpRequestTimeout { get; set; } = 30000;

        protected abstract Task<TTransport> CreateTransportAsync();

        protected abstract Task<TProtocol> CreateProtocolAsync(TTransport transport);

        protected abstract TOpenSessionReq CreateSessionRequest();

        internal abstract SchemaParser SchemaParser { get; }

        internal abstract IArrowArrayStream NewReader<T>(T statement, Schema schema) where T : HiveServer2Statement;

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new NotImplementedException();
        }

        internal static async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds, ActivitySource? activitySource)
        {
            await TraceAsync(activitySource, async (activity) =>
            {
                TGetOperationStatusResp? statusResponse = null;
                do
                {
                    if (statusResponse != null) { await Task.Delay(pollTimeMilliseconds); }
                    TGetOperationStatusReq request = new(operationHandle);
                    activity?.AddEvent(new ActivityEvent("getOperationStatus.start"));
                    statusResponse = await client.GetOperationStatus(request);
                    activity?.AddEvent(new ActivityEvent("getOperationStatus.stop", tags: new([new("statusResponse.operationState", statusResponse.OperationState.ToString())])));
                } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);
            });
        }

        private string GetInfoTypeStringValue(TGetInfoType infoType)
        {
            return Trace((activity) =>
            {
                TGetInfoReq req = new()
                {
                    SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
                    InfoType = infoType,
                };

                TGetInfoResp getInfoResp = Client.GetInfo(req).Result;
                if (getInfoResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new HiveServer2Exception(getInfoResp.Status.ErrorMessage)
                        .SetNativeError(getInfoResp.Status.ErrorCode)
                        .SetSqlState(getInfoResp.Status.SqlState);
                }

                string stringValue = getInfoResp.InfoValue.StringValue;
                return stringValue;
            });
        }

        protected override void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_client != null)
                    {
                        TCloseSessionReq r6 = new(SessionHandle);
                        _client.CloseSession(r6).Wait();

                        _transport?.Close();
                        _client.Dispose();
                        _transport = null;
                        _client = null;
                    }
                }
                _disposed = true;
            }
            base.Dispose(disposing);
        }

        public override void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        internal static async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, ActivitySource? activitySource = null, CancellationToken cancellationToken = default)
        {
            return await TraceAsync(activitySource, async (activity) =>
            {
                TGetResultSetMetadataReq request = new(operationHandle);
                TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
                return response;
            });
        }
    }
}
