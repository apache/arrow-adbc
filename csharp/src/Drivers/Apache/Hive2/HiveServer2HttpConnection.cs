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
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Security;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2HttpConnection : HiveServer2Connection
    {
        private const string ProductVersionDefault = "1.0.0";
        private const string DriverName = "ADBC Hive Driver";
        private const string ArrowVersion = "1.0.0";
        private const string BasicAuthenticationScheme = "Basic";
        private readonly Lazy<string> _productVersion;
        private static readonly string s_userAgent = $"{DriverName.Replace(" ", "")}/{ProductVersionDefault}";

        protected override string GetProductVersionDefault() => ProductVersionDefault;

        protected override string ProductVersion => _productVersion.Value;

        public HiveServer2HttpConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
            ValidateProperties();
            _productVersion = new Lazy<string>(() => GetProductVersion(), LazyThreadSafetyMode.PublicationOnly);
        }

        private void ValidateProperties()
        {
            ValidateAuthentication();
            ValidateConnection();
            ValidateOptions();
        }

        private void ValidateAuthentication()
        {
            // Validate authentication parameters
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(HiveServer2Parameters.AuthType, out string? authType);
            if (!HiveServer2AuthTypeParser.TryParse(authType, out HiveServer2AuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(HiveServer2Parameters.AuthType, authType, $"Unsupported {HiveServer2Parameters.AuthType} value.");
            }
            switch (authTypeValue)
            {
                case HiveServer2AuthType.Basic:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameter '{HiveServer2Parameters.AuthType}' is set to '{HiveServer2AuthTypeConstants.Basic}' but parameters '{AdbcOptions.Username}' or '{AdbcOptions.Password}' are not set. Please provide a values for these parameters.",
                            nameof(Properties));
                    break;
                case HiveServer2AuthType.UsernameOnly:
                    if (string.IsNullOrWhiteSpace(username))
                        throw new ArgumentException(
                            $"Parameter '{HiveServer2Parameters.AuthType}' is set to '{HiveServer2AuthTypeConstants.UsernameOnly}' but parameter '{AdbcOptions.Username}' is not set. Please provide a values for this parameter.",
                            nameof(Properties));
                    break;
                case HiveServer2AuthType.None:
                    break;
                case HiveServer2AuthType.Empty:
                    if (string.IsNullOrWhiteSpace(username) || string.IsNullOrWhiteSpace(password))
                        throw new ArgumentException(
                            $"Parameters must include valid authentication settings. Please provide '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.",
                            nameof(Properties));
                    break;
                default:
                    throw new ArgumentOutOfRangeException(HiveServer2Parameters.AuthType, authType, $"Unsupported {HiveServer2Parameters.AuthType} value.");
            }
        }

        private void ValidateConnection()
        {
            // HostName or Uri is required parameter
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);
            Properties.TryGetValue(HiveServer2Parameters.HostName, out string? hostName);
            if ((Uri.CheckHostName(hostName) == UriHostNameType.Unknown)
                && (string.IsNullOrEmpty(uri) || !Uri.TryCreate(uri, UriKind.Absolute, out Uri? _)))
            {
                throw new ArgumentException(
                    $"Required parameter '{HiveServer2Parameters.HostName}' or '{AdbcOptions.Uri}' is missing or invalid. Please provide a valid hostname or URI for the data source.",
                    nameof(Properties));
            }

            // Validate port range
            Properties.TryGetValue(HiveServer2Parameters.Port, out string? port);
            if (int.TryParse(port, out int portNumber) && (portNumber <= IPEndPoint.MinPort || portNumber > IPEndPoint.MaxPort))
                throw new ArgumentOutOfRangeException(
                    nameof(Properties),
                    port,
                    $"Parameter '{HiveServer2Parameters.Port}' value is not in the valid range of 1 .. {IPEndPoint.MaxPort}.");

            // Ensure the parameters will produce a valid address
            Properties.TryGetValue(HiveServer2Parameters.Path, out string? path);
            _ = new HttpClient()
            {
                BaseAddress = GetBaseAddress(uri, hostName, path, port, HiveServer2Parameters.HostName)
            };
        }

        private void ValidateOptions()
        {
            Properties.TryGetValue(HiveServer2Parameters.DataTypeConv, out string? dataTypeConv);
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);
            Properties.TryGetValue(HiveServer2Parameters.TLSOptions, out string? tlsOptions);
            TlsOptions = TlsOptionsParser.Parse(tlsOptions);
            Properties.TryGetValue(HiveServer2Parameters.ConnectTimeoutMilliseconds, out string? connectTimeoutMs);
            if (connectTimeoutMs != null)
            {
                ConnectTimeoutMilliseconds = int.TryParse(connectTimeoutMs, NumberStyles.Integer, CultureInfo.InvariantCulture, out int connectTimeoutMsValue) && (connectTimeoutMsValue >= 0)
                    ? connectTimeoutMsValue
                    : throw new ArgumentOutOfRangeException(HiveServer2Parameters.ConnectTimeoutMilliseconds, connectTimeoutMs, $"must be a value of 0 (infinite) or between 1 .. {int.MaxValue}. default is 30000 milliseconds.");
            }
        }

        public override AdbcStatement CreateStatement()
        {
            return new HiveServer2Statement(this);
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema) => new HiveServer2Reader(
            statement,
            schema,
            dataTypeConversion: statement.Connection.DataTypeConversion,
            enableBatchSizeStopCondition: false);

        protected override TTransport CreateTransport()
        {
            // Assumption: parameters have already been validated.
            Properties.TryGetValue(HiveServer2Parameters.HostName, out string? hostName);
            Properties.TryGetValue(HiveServer2Parameters.Path, out string? path);
            Properties.TryGetValue(HiveServer2Parameters.Port, out string? port);
            Properties.TryGetValue(HiveServer2Parameters.AuthType, out string? authType);
            if (!HiveServer2AuthTypeParser.TryParse(authType, out HiveServer2AuthType authTypeValue))
            {
                throw new ArgumentOutOfRangeException(HiveServer2Parameters.AuthType, authType, $"Unsupported {HiveServer2Parameters.AuthType} value.");
            }
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);

            Uri baseAddress = GetBaseAddress(uri, hostName, path, port, HiveServer2Parameters.HostName);
            AuthenticationHeaderValue? authenticationHeaderValue = GetAuthenticationHeaderValue(authTypeValue, username, password);

            HttpClientHandler httpClientHandler = NewHttpClientHandler();
            httpClientHandler.AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;
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

        private static AuthenticationHeaderValue? GetAuthenticationHeaderValue(HiveServer2AuthType authType, string? username, string? password)
        {
            if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password) && (authType == HiveServer2AuthType.Empty || authType == HiveServer2AuthType.Basic))
            {
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
            }
            else if (!string.IsNullOrEmpty(username) && (authType == HiveServer2AuthType.Empty || authType == HiveServer2AuthType.UsernameOnly))
            {
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:")));
            }
            else if (authType == HiveServer2AuthType.None)
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
            var req = new TOpenSessionReq(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V11)
            {
                CanUseMultipleCatalogs = true,
            };
            return req;
        }

        protected override void SetPrecisionScaleAndTypeName(
            short colType,
            string typeName,
            TableInfo? tableInfo,
            int columnSize,
            int decimalDigits)
        {
            // Keep the original type name
            tableInfo?.TypeName.Add(typeName);
            switch (colType)
            {
                case (short)ColumnTypeId.DECIMAL:
                case (short)ColumnTypeId.NUMERIC:
                    {
                        // Precision/scale is provide in the API call.
                        SqlDecimalParserResult result = SqlTypeNameParser<SqlDecimalParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(columnSize);
                        tableInfo?.Scale.Add((short)decimalDigits);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                case (short)ColumnTypeId.CHAR:
                case (short)ColumnTypeId.NCHAR:
                case (short)ColumnTypeId.VARCHAR:
                case (short)ColumnTypeId.LONGVARCHAR:
                case (short)ColumnTypeId.LONGNVARCHAR:
                case (short)ColumnTypeId.NVARCHAR:
                    {
                        // Precision is provide in the API call.
                        SqlCharVarcharParserResult result = SqlTypeNameParser<SqlCharVarcharParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(columnSize);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                default:
                    {
                        SqlTypeNameParserResult result = SqlTypeNameParser<SqlTypeNameParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(null);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }
            }
        }

        protected override ColumnsMetadataColumnNames GetColumnsMetadataColumnNames()
        {
            return new ColumnsMetadataColumnNames()
            {
                TableCatalog = TableCat,
                TableSchema = TableSchem,
                TableName = TableName,
                ColumnName = ColumnName,
                DataType = DataType,
                TypeName = TypeName,
                Nullable = Nullable,
                ColumnDef = ColumnDef,
                OrdinalPosition = OrdinalPosition,
                IsNullable = IsNullable,
                IsAutoIncrement = IsAutoIncrement,
                ColumnSize = ColumnSize,
                DecimalDigits = DecimalDigits,
            };
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle, cancellationToken: cancellationToken);

        protected internal override int PositionRequiredOffset => 0;

        protected override string InfoDriverName => DriverName;

        protected override string InfoDriverArrowVersion => ArrowVersion;

        protected override bool IsColumnSizeValidForDecimal => false;

        protected override bool GetObjectsPatternsRequireLowerCase => false;

        internal override SchemaParser SchemaParser => new HiveServer2SchemaParser();

        internal HiveServer2TransportType Type => HiveServer2TransportType.Http;

        protected override int ColumnMapIndexOffset => 1;
    }
}
