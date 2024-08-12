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
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
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
        private const string userAgent = "AdbcExperimental/0.0";

        protected TOperationHandle? operationHandle;
        internal TTransport? _transport;
        private TCLIService.Client? _client;
        private readonly Lazy<string> _vendorVersion;
        private readonly Lazy<string> _vendorName;

        public HiveServer2Connection(IReadOnlyDictionary<string, string> properties)
        {
            Properties = properties;
            // Note: "LazyThreadSafetyMode.PublicationOnly" is thread-safe initialization where
            // the first successful thread sets the value. If an exception is thrown, initialization
            // will retry until it successfully returns a value without an exception.
            // https://learn.microsoft.com/en-us/dotnet/framework/performance/lazy-initialization#exceptions-in-lazy-objects
            _vendorVersion = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_VER), LazyThreadSafetyMode.PublicationOnly);
            _vendorName = new Lazy<string>(() => GetInfoTypeStringValue(TGetInfoType.CLI_DBMS_NAME), LazyThreadSafetyMode.PublicationOnly);
        }

        public TCLIService.Client Client
        {
            get { return _client ?? throw new InvalidOperationException("connection not open"); }
        }

        public string VendorVersion => _vendorVersion.Value;

        public string VendorName => _vendorName.Value;

        public IReadOnlyDictionary<string, string> Properties { get; }

        public async Task OpenAsync()
        {
            TTransport transport = await CreateTransportAsync();
            TProtocol protocol = await CreateProtocolAsync(transport);
            _transport = protocol.Transport;
            _client = new TCLIService.Client(protocol);
            TOpenSessionReq request = CreateSessionRequest();
            TOpenSessionResp? session = await Client.OpenSession(request);
            SessionHandle = session.SessionHandle;
        }

        public TSessionHandle? SessionHandle { get; private set; }

        protected abstract Task<TTransport> CreateTransportAsync();
        protected abstract Task<TProtocol> CreateProtocolAsync(TTransport transport);
        protected abstract TOpenSessionReq CreateSessionRequest();
        public abstract SchemaParser SchemaParser { get; }

        public abstract IArrowArrayStream NewReader<T>(T statement, Schema schema) where T : HiveServer2Statement;

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            throw new NotImplementedException();
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new NotImplementedException();
        }

        internal static async Task PollForResponseAsync(TOperationHandle operationHandle, TCLIService.IAsync client, int pollTimeMilliseconds)
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

            return getInfoResp.InfoValue.StringValue;
        }

        public override void Dispose()
        {
            if (_client != null)
            {
                TCloseSessionReq r6 = new TCloseSessionReq(SessionHandle);
                _client.CloseSession(r6).Wait();

                _transport?.Close();
                _client.Dispose();
                _transport = null;
                _client = null;
            }
        }

        public static async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TOperationHandle operationHandle, TCLIService.IAsync client, CancellationToken cancellationToken = default)
        {
            TGetResultSetMetadataReq request = new(operationHandle);
            TGetResultSetMetadataResp response = await client.GetResultSetMetadata(request, cancellationToken);
            return response;
        }

        protected static Uri GetBaseAddress(string? uri, string? hostName, string? path, string? port)
        {
            // Uri property takes precedent.
            if (!string.IsNullOrWhiteSpace(uri))
            {
                var uriValue = new Uri(uri);
                if (uriValue.Scheme != Uri.UriSchemeHttp && uriValue.Scheme != Uri.UriSchemeHttps)
                    throw new ArgumentOutOfRangeException(
                        AdbcOptions.Uri,
                        uri,
                        $"Unsupported scheme '{uriValue.Scheme}'");
                return uriValue;
            }

            bool isPortSet = !string.IsNullOrEmpty(port);
            bool isValidPortNumber = int.TryParse(port, out int portNumber) && portNumber > 0;
            bool isDefaultHttpsPort = !isPortSet || (isValidPortNumber && portNumber == 443);
            string uriScheme = isDefaultHttpsPort ? Uri.UriSchemeHttps : Uri.UriSchemeHttp;
            int uriPort;
            if (!isPortSet)
                uriPort = -1;
            else if (isValidPortNumber)
                uriPort = portNumber;
            else
                throw new ArgumentOutOfRangeException(nameof(port), portNumber, $"Port number is not in a valid range.");

            Uri baseAddress = new UriBuilder(uriScheme, hostName, uriPort, path).Uri;
            return baseAddress;
        }

        protected static AuthenticationHeaderValue GetAuthenticationHeaderValue(string? authType, string? token, string? username, string? password)
        {
            bool isValidAuthType = Enum.TryParse(authType, out SparkAuthType authTypeValue);
            if (!string.IsNullOrEmpty(token) && (!isValidAuthType || authTypeValue == SparkAuthType.Token))
            {
                return new AuthenticationHeaderValue("Bearer", token);
            }
            else if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password) && (!isValidAuthType || authTypeValue == SparkAuthType.Basic))
            {
                return new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
            }
            else
            {
                throw new AdbcException("Missing connection properties. Must contain 'token' or 'username' and 'password'");
            }
        }

    }
}
