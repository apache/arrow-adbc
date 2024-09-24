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
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Thrift;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkHttpConnection : SparkConnection
    {
        private const string BasicAuthenticationScheme = "Basic";
        private const string BearerAuthenticationScheme = "Bearer";

        public SparkHttpConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        protected override void ValidateAuthentication()
        {
            // Validate authentication parameters
            Properties.TryGetValue(SparkParameters.Token, out string? token);
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            bool isValidAuthType = SparkAuthTypeConstants.TryParse(authType, out SparkAuthType authTypeValue);
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
                default:
                    throw new ArgumentOutOfRangeException(SparkParameters.AuthType, authType, $"Unsupported {SparkParameters.AuthType} value.");
            }
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
                BaseAddress = GetBaseAddress(uri, hostName, path, port)
            };
        }

        protected override void ValidateOptions()
        {
            Properties.TryGetValue(SparkParameters.DataTypeConv, out string? dataTypeConv);
            SparkDataTypeConversionConstants.TryParse(dataTypeConv, out SparkDataTypeConversion dataTypeConversionValue);
            DataTypeConversion = dataTypeConversionValue switch
            {
                SparkDataTypeConversion.None => dataTypeConversionValue!,
                _ => throw new NotImplementedException($"Invalid or unsupported data type conversion option: '{dataTypeConv}'. Supported values: {SparkDataTypeConversionConstants.SupportedList}"),
            };
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema) => new HiveServer2Reader(statement, schema);

        protected override Task<TTransport> CreateTransportAsync()
        {
            foreach (var property in Properties.Keys)
            {
                Trace.TraceError($"key = {property} value = {Properties[property]}");
            }

            // Assumption: parameters have already been validated.
            Properties.TryGetValue(SparkParameters.HostName, out string? hostName);
            Properties.TryGetValue(SparkParameters.Path, out string? path);
            Properties.TryGetValue(SparkParameters.Port, out string? port);
            Properties.TryGetValue(SparkParameters.AuthType, out string? authType);
            bool isValidAuthType = SparkAuthTypeConstants.TryParse(authType, out SparkAuthType authTypeValue);
            Properties.TryGetValue(SparkParameters.Token, out string? token);
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);

            Uri baseAddress = GetBaseAddress(uri, hostName, path, port);
            AuthenticationHeaderValue? authenticationHeaderValue = GetAuthenticationHeaderValue(authTypeValue, token, username, password);

            HttpClient httpClient = new();
            httpClient.BaseAddress = baseAddress;
            httpClient.DefaultRequestHeaders.Authorization = authenticationHeaderValue;
            httpClient.DefaultRequestHeaders.UserAgent.ParseAdd(s_userAgent);
            httpClient.DefaultRequestHeaders.AcceptEncoding.Clear();
            httpClient.DefaultRequestHeaders.AcceptEncoding.Add(new StringWithQualityHeaderValue("identity"));
            httpClient.DefaultRequestHeaders.ExpectContinue = false;

            TConfiguration config = new();
            ThriftHttpTransport transport = new(httpClient, config);
            return Task.FromResult<TTransport>(transport);
        }

        private static AuthenticationHeaderValue? GetAuthenticationHeaderValue(SparkAuthType authType, string? token, string? username, string? password)
        {
            if (!string.IsNullOrEmpty(token) && (authType == SparkAuthType.Empty || authType == SparkAuthType.Token))
            {
                return new AuthenticationHeaderValue(BearerAuthenticationScheme, token);
            }
            else if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password) && (authType == SparkAuthType.Empty || authType == SparkAuthType.Basic))
            {
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}")));
            }
            else if (!string.IsNullOrEmpty(username) && (authType == SparkAuthType.Empty || authType == SparkAuthType.UsernameOnly))
            {
                return new AuthenticationHeaderValue(BasicAuthenticationScheme, Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:")));
            }
            else if (authType == SparkAuthType.None)
            {
                return null;
            }
            else
            {
                throw new AdbcException("Missing connection properties. Must contain 'token' or 'username' and 'password'");
            }
        }

        protected override async Task<TProtocol> CreateProtocolAsync(TTransport transport)
        {
            Trace.TraceError($"create protocol with {Properties.Count} properties.");

            if (!transport.IsOpen) await transport.OpenAsync(CancellationToken.None);
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

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected override Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected override Task<TRowSet> GetRowSetAsync(TGetColumnsResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected override Task<TRowSet> GetRowSetAsync(TGetTablesResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected override Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected override Task<TRowSet> GetRowSetAsync(TGetSchemasResp response) =>
            FetchResultsAsync(response.OperationHandle);

        private async Task<TRowSet> FetchResultsAsync(TOperationHandle operationHandle, long batchSize = BatchSizeDefault, CancellationToken cancellationToken = default)
        {
            await PollForResponseAsync(operationHandle, Client, PollTimeMillisecondsDefault);
            TFetchResultsResp fetchResp = await FetchNextAsync(operationHandle, Client, batchSize, cancellationToken);
            if (fetchResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(fetchResp.Status.ErrorMessage)
                    .SetNativeError(fetchResp.Status.ErrorCode)
                    .SetSqlState(fetchResp.Status.SqlState);
            }
            return fetchResp.Results;
        }

        internal static async Task<TFetchResultsResp> FetchNextAsync(TOperationHandle operationHandle, TCLIService.IAsync client, long batchSize, CancellationToken cancellationToken = default)
        {
            TFetchResultsReq request = new(operationHandle, TFetchOrientation.FETCH_NEXT, batchSize);
            TFetchResultsResp response = await client.FetchResults(request, cancellationToken);
            return response;
        }

        internal override SchemaParser SchemaParser => new HiveServer2SchemaParser();

        internal override SparkServerType ServerType => SparkServerType.Http;

        internal class HiveServer2SchemaParser : SchemaParser
        {
            public override IArrowType GetArrowType(TPrimitiveTypeEntry thriftType)
            {
                return thriftType.Type switch
                {
                    TTypeId.BIGINT_TYPE => Int64Type.Default,
                    TTypeId.BINARY_TYPE => BinaryType.Default,
                    TTypeId.BOOLEAN_TYPE => BooleanType.Default,
                    TTypeId.DOUBLE_TYPE
                    or TTypeId.FLOAT_TYPE => DoubleType.Default,
                    TTypeId.INT_TYPE => Int32Type.Default,
                    TTypeId.SMALLINT_TYPE => Int16Type.Default,
                    TTypeId.TINYINT_TYPE => Int8Type.Default,
                    TTypeId.CHAR_TYPE
                    or TTypeId.DATE_TYPE
                    or TTypeId.DECIMAL_TYPE
                    or TTypeId.NULL_TYPE
                    or TTypeId.STRING_TYPE
                    or TTypeId.TIMESTAMP_TYPE
                    or TTypeId.VARCHAR_TYPE
                    or TTypeId.INTERVAL_DAY_TIME_TYPE
                    or TTypeId.INTERVAL_YEAR_MONTH_TYPE
                    or TTypeId.ARRAY_TYPE
                    or TTypeId.MAP_TYPE
                    or TTypeId.STRUCT_TYPE
                    or TTypeId.UNION_TYPE
                    or TTypeId.USER_DEFINED_TYPE => StringType.Default,
                    TTypeId.TIMESTAMPLOCALTZ_TYPE => throw new NotImplementedException(),
                    _ => throw new NotImplementedException(),
                };
            }
        }
    }
}
