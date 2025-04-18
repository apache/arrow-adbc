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
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    internal class DatabricksConnection : SparkHttpConnection
    {
        public DatabricksConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        /// <summary>
        /// Gets a value indicating whether to retry requests that receive a 503 response with a Retry-After header.
        /// </summary>
        protected bool TemporarilyUnavailableRetry { get; private set; } = true;

        /// <summary>
        /// Gets the maximum total time in seconds to retry 503 responses before failing.
        /// </summary>
        protected int TemporarilyUnavailableRetryTimeout { get; private set; } = 900;

        protected override HttpMessageHandler CreateHttpHandler()
        {
            var baseHandler = base.CreateHttpHandler();
            if (TemporarilyUnavailableRetry)
            {
                return new RetryHttpHandler(baseHandler, TemporarilyUnavailableRetryTimeout);
            }
            return baseHandler;
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, TGetResultSetMetadataResp? metadataResp = null)
        {
            // Get result format from metadata response if available
            TSparkRowSetType resultFormat = TSparkRowSetType.ARROW_BASED_SET;
            bool isLz4Compressed = false;

            DatabricksStatement? databricksStatement = statement as DatabricksStatement;

            if (databricksStatement == null)
            {
                throw new InvalidOperationException("Cannot obtain a reader for Databricks");
            }

            if (metadataResp != null)
            {
                if (metadataResp.__isset.resultFormat)
                {
                    resultFormat = metadataResp.ResultFormat;
                }

                if (metadataResp.__isset.lz4Compressed)
                {
                    isLz4Compressed = metadataResp.Lz4Compressed;
                }
            }

            // Choose the appropriate reader based on the result format
            if (resultFormat == TSparkRowSetType.URL_BASED_SET)
            {
                return new CloudFetchReader(databricksStatement, schema, isLz4Compressed);
            }
            else
            {
                return new DatabricksReader(databricksStatement, schema, isLz4Compressed);
            }
        }

        internal override SchemaParser SchemaParser => new DatabricksSchemaParser();

        public override AdbcStatement CreateStatement()
        {
            DatabricksStatement statement = new DatabricksStatement(this);
            return statement;
        }

        protected override TOpenSessionReq CreateSessionRequest()
        {
            var req = new TOpenSessionReq
            {
                Client_protocol = TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                Client_protocol_i64 = (long)TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7,
                CanUseMultipleCatalogs = true,
            };
            return req;
        }

        protected override void ValidateOptions()
        {
             base.ValidateOptions();

            bool tempUnavailableRetryValue = true; // Default to enabled
            // Parse retry configuration parameters
            if(Properties.TryGetValue(DatabricksParameters.TemporarilyUnavailableRetry, out string? tempUnavailableRetryStr))
            {
                throw new ArgumentOutOfRangeException(DatabricksParameters.TemporarilyUnavailableRetry, tempUnavailableRetryStr,
                    $"must be a value of false (disabled) or true (enabled). Default is 1.");
            }
            TemporarilyUnavailableRetry = tempUnavailableRetryValue;

            if(Properties.TryGetValue(DatabricksParameters.TemporarilyUnavailableRetryTimeout, out string? tempUnavailableRetryTimeoutStr))
            {
                if (!int.TryParse(tempUnavailableRetryTimeoutStr, out int tempUnavailableRetryTimeoutValue) ||
                    tempUnavailableRetryTimeoutValue < 0)
                {
                    throw new ArgumentOutOfRangeException(DatabricksParameters.TemporarilyUnavailableRetryTimeout, tempUnavailableRetryTimeoutStr,
                        $"must be a value of 0 (retry indefinitely) or a positive integer representing seconds. Default is 900 seconds (15 minutes).");
                }
                TemporarilyUnavailableRetryTimeout = tempUnavailableRetryTimeoutValue;
            }
            else
            {
                TemporarilyUnavailableRetryTimeout = 900; // Default to 15 minutes
            }
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected internal override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetPrimaryKeysResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);

        protected override Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected internal override Task<TRowSet> GetRowSetAsync(TGetPrimaryKeysResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
    }
}
