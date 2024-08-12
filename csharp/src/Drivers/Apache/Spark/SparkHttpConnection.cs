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
        public SparkHttpConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        public override IArrowArrayStream NewReader<T>(T statement, Schema schema) => new HiveServer2Reader(statement, schema);

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
            Properties.TryGetValue(SparkParameters.Token, out string? token);
            Properties.TryGetValue(AdbcOptions.Username, out string? username);
            Properties.TryGetValue(AdbcOptions.Password, out string? password);
            Properties.TryGetValue(AdbcOptions.Uri, out string? uri);

            Uri baseAddress = GetBaseAddress(uri, hostName, path, port);
            AuthenticationHeaderValue authenticationHeaderValue = GetAuthenticationHeaderValue(authType, token, username, password);

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

        private static AuthenticationHeaderValue GetAuthenticationHeaderValue(string? authType, string? token, string? username, string? password)
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

        public override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        public override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        public override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        public override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        public override Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response) =>
            FetchResultsAsync(response.OperationHandle);
        public override Task<TRowSet> GetRowSetAsync(TGetColumnsResp response) =>
            FetchResultsAsync(response.OperationHandle);
        public override Task<TRowSet> GetRowSetAsync(TGetTablesResp response) =>
            FetchResultsAsync(response.OperationHandle);
        public override Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response) =>
            FetchResultsAsync(response.OperationHandle);
        public override Task<TRowSet> GetRowSetAsync(TGetSchemasResp response) =>
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

        public override SchemaParser SchemaParser => new HiveServer2SchemaParser();

        public override SparkServerType ServerType => SparkServerType.Http;

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
