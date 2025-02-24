﻿/*
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
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal class ImpalaHttpConnection : ImpalaConnection
    {
        private const string BasicAuthenticationScheme = "Basic";

        public ImpalaHttpConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        protected override void ValidateAuthentication()
        {
            // Validate authentication parameters
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(ImpalaParameters.AuthType, out string? authType);
            bool isValidAuthType = ImpalaAuthTypeParser.TryParse(authType, out ImpalaAuthType authTypeValue);
            switch (authTypeValue)
            {
                case ImpalaAuthType.Basic:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameter '{ImpalaParameters.AuthType}' is set to '{ImpalaAuthTypeConstants.Basic}' but parameters '{AdbcOptions.Username}' or '{AdbcOptions.Password}' are not set. Please provide a values for these parameters.",
                            nameof(Properties));
                    break;
                case ImpalaAuthType.UsernameOnly:
                    if (string.IsNullOrWhiteSpace(username))
                        throw new ArgumentException(
                            $"Parameter '{ImpalaParameters.AuthType}' is set to '{ImpalaAuthTypeConstants.UsernameOnly}' but parameter '{AdbcOptions.Username}' is not set. Please provide a values for this parameter.",
                            nameof(Properties));
                    break;
                case ImpalaAuthType.None:
                    break;
                case ImpalaAuthType.Empty:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameters must include valid authentiation settings. Please provide '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.",
                            nameof(Properties));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(ImpalaParameters.AuthType, authType, $"Unsupported {ImpalaParameters.AuthType} value.");
            }
        }

        protected override void ValidateConnection()
        {
            // HostName or Uri is required parameter
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);
            Properties.TryGetValue(ImpalaParameters.HostName, out string? hostName);
            if ((Uri.CheckHostName(hostName) == UriHostNameType.Unknown)
                && (string.IsNullOrEmpty(uri) || !Uri.TryCreate(uri, UriKind.Absolute, out Uri? _)))
            {
                throw new ArgumentException(
                    $"Required parameter '{ImpalaParameters.HostName}' or '{AdbcOptions.Uri}' is missing or invalid. Please provide a valid hostname or URI for the data source.",
                    nameof(Properties));
            }

            // Validate port range
            Properties.TryGetValue(ImpalaParameters.Port, out string? port);
            if (int.TryParse(port, out int portNumber) && (portNumber <= IPEndPoint.MinPort || portNumber > IPEndPoint.MaxPort))
                throw new ArgumentOutOfRangeException(
                    nameof(Properties),
                    port,
                    $"Parameter '{ImpalaParameters.Port}' value is not in the valid range of 1 .. {IPEndPoint.MaxPort}.");

            // Ensure the parameters will produce a valid address
            Properties.TryGetValue(ImpalaParameters.Path, out string? path);
            _ = new HttpClient()
            {
                BaseAddress = GetBaseAddress(uri, hostName, path, port)
            };
        }

        protected override void ValidateOptions()
        {
            Properties.TryGetValue(ImpalaParameters.DataTypeConv, out string? dataTypeConv);
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);
            Properties.TryGetValue(ImpalaParameters.TLSOptions, out string? tlsOptions);
            TlsOptions = TlsOptionsParser.Parse(tlsOptions);
            Properties.TryGetValue(ImpalaParameters.ConnectTimeoutMilliseconds, out string? connectTimeoutMs);
            if (connectTimeoutMs != null)
            {
                ConnectTimeoutMilliseconds = int.TryParse(connectTimeoutMs, NumberStyles.Integer, CultureInfo.InvariantCulture, out int connectTimeoutMsValue) && (connectTimeoutMsValue >= 0)
                    ? connectTimeoutMsValue
                    : throw new ArgumentOutOfRangeException(ImpalaParameters.ConnectTimeoutMilliseconds, connectTimeoutMs, $"must be a value of 0 (infinite) or between 1 .. {int.MaxValue}. default is 30000 milliseconds.");
            }
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema) => new HiveServer2Reader(statement, schema, dataTypeConversion: statement.Connection.DataTypeConversion);

        protected override TTransport CreateTransport()
        {
            // Assumption: parameters have already been validated.
            Properties.TryGetValue(ImpalaParameters.HostName, out string? hostName);
            Properties.TryGetValue(ImpalaParameters.Path, out string? path);
            Properties.TryGetValue(ImpalaParameters.Port, out string? port);
            Properties.TryGetValue(ImpalaParameters.AuthType, out string? authType);
            bool isValidAuthType = ImpalaAuthTypeParser.TryParse(authType, out ImpalaAuthType authTypeValue);
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);

            Uri baseAddress = GetBaseAddress(uri, hostName, path, port);
            AuthenticationHeaderValue? authenticationHeaderValue = GetAuthenticationHeaderValue(authTypeValue, username, password);

            HttpClientHandler httpClientHandler = NewHttpClientHandler();
            HttpClient httpClient = new(httpClientHandler);
            httpClient.BaseAddress = baseAddress;
            httpClient.DefaultRequestHeaders.Authorization = authenticationHeaderValue;
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(s_userAgent);
            httpClient.DefaultRequestHeaders.AcceptEncoding.Clear();
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
            httpClient.DefaultRequestHeaders.ExpectContinue = false;

            TConfiguration config = new();
            ThriftHttpTransport transport = new(httpClient, config)
            {
                // This value can only be set before the first call/request. So if a new value for query timeout
                // is set, we won't be able to update the value. Setting to ~infinite and relying on cancellation token
                // to ensure cancelled correctly.
                ConnectTimeout = int.MaxValue,
            };
            return transport;
        }

        private HttpClientHandler NewHttpClientHandler()
        {
            HttpClientHandler httpClientHandler = new();
            if (TlsOptions != HiveServer2TlsOption.Empty)
            {
                httpClientHandler.ServerCertificateCustomValidationCallback = (request, certificate, chain, policyErrors) =>
                {

                    if (policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateChainErrors) && !TlsOptions.HasFlag(HiveServer2TlsOption.AllowSelfSigned)) return false;
                    if (policyErrors.HasFlag(SslPolicyErrors.RemoteCertificateNameMismatch) && !TlsOptions.HasFlag(HiveServer2TlsOption.AllowHostnameMismatch)) return false;

                    return true;
                };
            }

            return httpClientHandler;
        }

        private static AuthenticationHeaderValue? GetAuthenticationHeaderValue(ImpalaAuthType authType, string? username, string? password)
        {
            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password) && (authType == ImpalaAuthType.Empty || authType == ImpalaAuthType.Basic))
            {
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
            }
            else if (!string.IsNullOrEmpty(username) && (authType == ImpalaAuthType.Empty || authType == ImpalaAuthType.UsernameOnly))
            {
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:")));
            }
            else if (authType == ImpalaAuthType.None)
            {
                return null;
            }
            else
            {
                throw new AdbcException("Missing connection properties. Must contain 'username' and 'password'");
            }
        }

        protected override async Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default)
        {
            if (!transport.IsOpen) await transport.OpenAsync(cancellationToken);
            return new TBinaryProtocol(transport);
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            return new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7)
            {
                CanUseMultipleCatalogs = true,
            };
        }

        internal override SchemaParser SchemaParser => new HiveServer2SchemaParser();

        internal override ImpalaServerType ServerType => ImpalaServerType.Http;

        protected override int ColumnMapIndexOffset => 0;
    }
}
