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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal abstract class HiveServer2Connection : AdbcConnection
    {
        internal const long BatchSizeDefault = 50000;
        internal const int PollTimeMillisecondsDefault = 500;
        private const int ConnectTimeoutMillisecondsDefault = 30000;
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

            if (properties.TryGetValue(ApacheParameters.QueryTimeoutSeconds, out string? queryTimeoutSecondsSettingValue))
            {
                if (ApacheUtility.QueryTimeoutIsValid(ApacheParameters.QueryTimeoutSeconds, queryTimeoutSecondsSettingValue, out int queryTimeoutSeconds))
                {
                    QueryTimeoutSeconds = queryTimeoutSeconds;
                }
            }
        }

        internal TCLIService.Client Client
        {
            get { return _client ?? throw new InvalidOperationException("connection not open"); }
        }

        internal string VendorVersion => _vendorVersion.Value;

        internal string VendorName => _vendorName.Value;

        protected internal int QueryTimeoutSeconds { get; set; } = ApacheUtility.QueryTimeoutSecondsDefault;

        internal IReadOnlyDictionary<string, string> Properties { get; }

        internal async Task OpenAsync()
        {
            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(ConnectTimeoutMilliseconds, ApacheUtility.TimeUnit.Milliseconds);
            try
            {
                TTransport transport = CreateTransport();
                TProtocol protocol = await CreateProtocolAsync(transport, cancellationToken);
                _transport = protocol.Transport;
                _client = new TCLIService.Client(protocol);
                TOpenSessionReq request = CreateSessionRequest();

                TOpenSessionResp? session = await Client.OpenSession(request, cancellationToken);

                // Explicitly check the session status
                if (session == null)
                {
                    throw new HiveServer2Exception("Unable to open session. Unknown error.");
                }
                else if (session.Status.StatusCode != TStatusCode.SUCCESS_STATUS)
                {
                    throw new HiveServer2Exception(session.Status.ErrorMessage)
                        .SetNativeError(session.Status.ErrorCode)
                        .SetSqlState(session.Status.SqlState);
                }

                SessionHandle = session.SessionHandle;
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The operation timed out while attempting to open a session. Please try increasing connect timeout.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                // Handle other exceptions if necessary
                throw new HiveServer2Exception($"An unexpected error occurred while opening the session. '{ex.Message}'", ex);
            }
        }

        internal TSessionHandle? SessionHandle { get; private set; }

        protected internal DataTypeConversion DataTypeConversion { get; set; } = DataTypeConversion.None;

        protected internal HiveServer2TlsOption TlsOptions { get; set; } = HiveServer2TlsOption.Empty;

        protected internal int ConnectTimeoutMilliseconds { get; set; } = ConnectTimeoutMillisecondsDefault;

        protected abstract TTransport CreateTransport();

        protected abstract Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default);

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

        internal static async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds, CancellationToken cancellationToken = default)
        {
            TGetOperationStatusResp? statusResponse = null;
            do
            {
                if (statusResponse != null) { await Task.Delay(pollTimeMilliseconds, cancellationToken); }
                TGetOperationStatusReq request = new(operationHandle);
                statusResponse = await client.GetOperationStatus(request, cancellationToken);
            } while (statusResponse.OperationState == TOperationState.PENDING_STATE || statusResponse.OperationState == TOperationState.RUNNING_STATE);

            // Must be in the finished state to be valid. If not, typically a server error or timeout has occurred.
            if (statusResponse.OperationState != TOperationState.FINISHED_STATE)
            {
                throw new HiveServer2Exception(statusResponse.ErrorMessage, AdbcStatusCode.InvalidState)
                    .SetSqlState(statusResponse.SqlState)
                    .SetNativeError(statusResponse.ErrorCode);
            }
        }

        private string GetInfoTypeStringValue(TGetInfoType infoType)
        {
            TGetInfoReq req = new()
            {
                SessionHandle = SessionHandle ?? throw new InvalidOperationException("session not created"),
                InfoType = infoType,
            };

            CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
            try
            {
                TGetInfoResp getInfoResp = Client.GetInfo(req, cancellationToken).Result;
                if (getInfoResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new HiveServer2Exception(getInfoResp.Status.ErrorMessage)
                        .SetNativeError(getInfoResp.Status.ErrorCode)
                        .SetSqlState(getInfoResp.Status.SqlState);
                }

                return getInfoResp.InfoValue.StringValue;
            }
            catch (Exception ex)
                when (ApacheUtility.ContainsException(ex, out OperationCanceledException? _) ||
                     (ApacheUtility.ContainsException(ex, out TTransportException? _) && cancellationToken.IsCancellationRequested))
            {
                throw new TimeoutException("The metadata query execution timed out. Consider increasing the query timeout value.", ex);
            }
            catch (Exception ex) when (ex is not HiveServer2Exception)
            {
                throw new HiveServer2Exception($"An unexpected error occurred while running metadata query. '{ex.Message}'", ex);
            }
        }

        public override void Dispose()
        {
            if (_client != null)
            {
                CancellationToken cancellationToken = ApacheUtility.GetCancellationToken(QueryTimeoutSeconds, ApacheUtility.TimeUnit.Seconds);
                TCloseSessionReq r6 = new(SessionHandle);
                _client.CloseSession(r6, cancellationToken).Wait();
                _transport?.Close();
                _client.Dispose();
                _transport = null;
                _client = null;
            }
        }

        internal static async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataReq request = new(operationHandle);
            TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
            return response;
        }
    }
}
