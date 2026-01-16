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
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;
using Thrift.Transport.Client;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkHttpConnection : SparkConnection
    {
        private const string BasicAuthenticationScheme = "Basic";
        private const string BearerAuthenticationScheme = "Bearer";
        private const string AnonymousAuthenticationScheme = "Anonymous";

        protected readonly HiveServer2ProxyConfigurator _proxyConfigurator;

        public SparkHttpConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
            _proxyConfigurator = HiveServer2ProxyConfigurator.FromProperties(properties);
        }

        protected override void ValidateAuthentication()
        {
            // Validate authentication parameters
            Properties.TryGetValue(SparkParameters.Token, out string? token);
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            if (!SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
            }
            switch (authTypeValue)
            {
                case SparkAuthType.Token:
                    if (string.IsNullOrWhiteSpace(token))
                        throw new ArgumentException(
                            $"Parameter '{SparkParameters.AuthType}' is set to '{SparkAuthTypeConstants.Token}' but parameter '{SparkParameters.Token}' is not set. Please provide a value for '{SparkParameters.Token}'.",
                            nameof(Properties));
                    break;
                case SparkAuthType.Basic:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameter '{SparkParameters.AuthType}' is set to '{SparkAuthTypeConstants.Basic}' but parameters '{AdbcOptions.Username}' or '{AdbcOptions.Password}' are not set. Please provide a values for these parameters.",
                            nameof(Properties));
                    break;
                case SparkAuthType.UsernameOnly:
                    if (string.IsNullOrWhiteSpace(username))
                        throw new ArgumentException(
                            $"Parameter '{SparkParameters.AuthType}' is set to '{SparkAuthTypeConstants.UsernameOnly}' but parameter '{AdbcOptions.Username}' is not set. Please provide a values for this parameter.",
                            nameof(Properties));
                    break;
                case SparkAuthType.None:
                    break;
                case SparkAuthType.Empty:
                    if (string.IsNullOrWhiteSpace(token) && (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password)))
                        throw new ArgumentException(
                            $"Parameters must include valid authentiation settings. Please provide either '{SparkParameters.Token}'; or '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.",
                            nameof(Properties));
                    break;

                case SparkAuthType.OAuth:
                    ValidateOAuthParameters();
                    break;
                default:
                    throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
            }
        }

        protected virtual void ValidateOAuthParameters()
        {
            Properties.TryGetValue(SparkParameters.AccessToken, out string? access_token);
            if (string.IsNullOrWhiteSpace(access_token))
                throw new ArgumentException(
                    $"Parameter '{SparkParameters.AuthType}' is set to '{SparkAuthTypeConstants.OAuth}' but parameter '{SparkParameters.AccessToken}' is not set. Please provide a value for '{SparkParameters.AccessToken}'.",
                    nameof(Properties));
        }

        protected override void ValidateConnection()
        {
            // HostName or Uri is required parameter
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);
            Properties.TryGetValue(SparkParameters.HostName, out string? hostName);
            if ((Uri.CheckHostName(hostName) == UriHostNameType.Unknown)
                && (string.IsNullOrEmpty(uri) || !Uri.TryCreate(uri, UriKind.Absolute, out Uri? _)))
            {
                throw new ArgumentException(
                    $"Required parameter '{SparkParameters.HostName}' or '{AdbcOptions.Uri}' is missing or invalid. Please provide a valid hostname or URI for the data source.",
                    nameof(Properties));
            }

            // Validate port range
            Properties.TryGetValue(SparkParameters.Port, out string? port);
            if (int.TryParse(port, out int portNumber) && (portNumber <= IPEndPoint.MinPort || portNumber > IPEndPoint.MaxPort))
                throw new ArgumentOutOfRangeException(
                    nameof(Properties),
                    port,
                    $"Parameter '{SparkParameters.Port}' value is not in the valid range of 1 .. {IPEndPoint.MaxPort}.");

            // Ensure the parameters will produce a valid address
            Properties.TryGetValue(SparkParameters.Path, out string? path);
            _ = new HttpClient()
            {
                BaseAddress = GetBaseAddress(uri, hostName, path, port, SparkParameters.HostName, TlsOptions.IsTlsEnabled)
            };
        }

        protected override void ValidateOptions()
        {
            Properties.TryGetValue(SparkParameters.DataTypeConv, out string? dataTypeConv);
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);
            Properties.TryGetValue(SparkParameters.ConnectTimeoutMilliseconds, out string? connectTimeoutMs);
            if (connectTimeoutMs != null)
            {
                ConnectTimeoutMilliseconds = int.TryParse(connectTimeoutMs, NumberStyles.Integer, CultureInfo.InvariantCulture, out int connectTimeoutMsValue) && (connectTimeoutMsValue >= 0)
                    ? connectTimeoutMsValue
                    : throw new ArgumentOutOfRangeException(SparkParameters.ConnectTimeoutMilliseconds, connectTimeoutMs, $"must be a value of 0 (infinite) or between 1 .. {int.MaxValue}. default is 30000 milliseconds.");
            }

            TlsOptions = HiveServer2TlsImpl.GetHttpTlsOptions(Properties);
        }

        internal override IArrowArrayStream NewReader<T>(
            T statement,
            Schema schema,
            IResponse response,
            TGetResultSetMetadataResp? metadataResp = null) => new HiveServer2Reader(statement, schema, response, dataTypeConversion: statement.Connection.DataTypeConversion);

        protected virtual HttpMessageHandler CreateHttpHandler()
        {
            return HiveServer2TlsImpl.NewHttpClientHandler(TlsOptions, _proxyConfigurator);
        }

        protected override TTransport CreateTransport()
        {
            Activity? activity = Activity.Current;

            // Assumption: parameters have already been validated.
            Properties.TryGetValue(SparkParameters.HostName, out string? hostName);
            Properties.TryGetValue(SparkParameters.Path, out string? path);
            Properties.TryGetValue(SparkParameters.Port, out string? port);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            if (!SparkAuthTypeParser.TryParse(authType, out SparkAuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
            }
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);

            Uri baseAddress = GetBaseAddress(uri, hostName, path, port, SparkParameters.HostName, TlsOptions.IsTlsEnabled);
            AuthenticationHeaderValue? authenticationHeaderValue = GetAuthenticationHeaderValue(authTypeValue);

            HttpClient httpClient = new(CreateHttpHandler());
            httpClient.BaseAddress = baseAddress;
            httpClient.DefaultRequestHeaders.Authorization = authenticationHeaderValue;
            string userAgent = GetUserAgent();
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(userAgent);
            httpClient.DefaultRequestHeaders.AcceptEncoding.Clear();
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
            httpClient.DefaultRequestHeaders.ExpectContinue = false;

            activity?.AddTag(ActivityKeys.Encrypted, baseAddress.Scheme == Uri.UriSchemeHttps);
            activity?.AddTag(ActivityKeys.TransportType, baseAddress.Scheme);
            activity?.AddTag(ActivityKeys.AuthType, authTypeValue.ToString());
            activity?.AddTag(ActivityKeys.Http.UserAgent, userAgent);
            activity?.AddTag(ActivityKeys.Http.Uri, baseAddress);

            TConfiguration config = GetTconfiguration();
            THttpTransport transport = new(httpClient, config)
            {
                // This value can only be set before the first call/request. So if a new value for query timeout
                // is set, we won't be able to update the value. Setting to ~infinite and relying on cancellation token
                // to ensure cancelled correctly.
                ConnectTimeout = int.MaxValue,
            };
            return transport;
        }

        protected virtual AuthenticationHeaderValue? GetAuthenticationHeaderValue(SparkAuthType authType)
        {
            Activity? activity = Activity.Current;

            Properties.TryGetValue(SparkParameters.Token, out string? token);
            Properties.TryGetValue(SparkParameters.AccessToken, out string? access_token);
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            if (!string.IsNullOrEmpty(token) && (authType == SparkAuthType.Empty || authType == SparkAuthType.Token))
            {
                activity?.AddTag(ActivityKeys.Http.AuthScheme, BearerAuthenticationScheme);
                return new AuthenticationHeaderValue(BearerAuthenticationScheme, token);
            }
            else if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password) && (authType == SparkAuthType.Empty || authType == SparkAuthType.Basic))
            {
                activity?.AddTag(ActivityKeys.Http.AuthScheme, BasicAuthenticationScheme);
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
            }
            else if (!string.IsNullOrEmpty(username) && (authType == SparkAuthType.Empty || authType == SparkAuthType.UsernameOnly))
            {
                activity?.AddTag(ActivityKeys.Http.AuthScheme, BasicAuthenticationScheme);
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:")));
            }
            else if (!string.IsNullOrEmpty(access_token) && authType == SparkAuthType.OAuth)
            {
                activity?.AddTag(ActivityKeys.Http.AuthScheme, BearerAuthenticationScheme);
                return new AuthenticationHeaderValue(BearerAuthenticationScheme, access_token);
            }
            else if (authType == SparkAuthType.None)
            {
                activity?.AddTag(ActivityKeys.Http.AuthScheme, AnonymousAuthenticationScheme);
                return null;
            }
            else
            {
                throw new AdbcException("Missing connection properties. Must contain 'token' or 'username' and 'password'");
            }
        }

        protected override async Task<TProtocol> CreateProtocolAsync(TTransport transport, CancellationToken cancellationToken = default)
        {
            if (!transport.IsOpen) await transport.OpenAsync(cancellationToken);
            return new TBinaryProtocol(transport);
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            var req = new TOpenSessionReq
            {
                Client_protocol = TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10,
                CanUseMultipleCatalogs = true,
            };
            return req;
        }

        internal override SchemaParser SchemaParser => new HiveServer2SchemaParser();

        internal override SparkServerType ServerType => SparkServerType.Http;

        public override string AssemblyVersion => s_assemblyVersion;

        public override string AssemblyName => s_assemblyName;

        protected override IEnumerable<TProtocolVersion> FallbackProtocolVersions => new[]
        {
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V9,
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V8,
            TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V7
        };

        private string GetUserAgent()
        {
            // Build the base user agent string with Thrift version
            string thriftVersion = GetThriftVersion();
            string thriftComponent = string.IsNullOrEmpty(thriftVersion) ? "Thrift" : $"Thrift/{thriftVersion}";
            string baseUserAgent = $"{DriverName.Replace(" ", "")}/{ProductVersionDefault} {thriftComponent}";

            // Check if a client has provided a user-agent entry
            if (Properties.TryGetValue(SparkParameters.UserAgentEntry, out string? userAgentEntry) && !string.IsNullOrWhiteSpace(userAgentEntry))
            {
                return $"{baseUserAgent} {userAgentEntry}";
            }

            return baseUserAgent;
        }

        private string GetThriftVersion()
        {
            try
            {
                var thriftAssembly = typeof(TProtocol).Assembly;
                var version = thriftAssembly.GetName().Version;
                return version != null ? $"{version.Major}.{version.Minor}.{version.Build}" : "";
            }
            catch
            {
                // Return empty string if there's any issue retrieving the assembly version
                return "";
            }
        }
    }
}
