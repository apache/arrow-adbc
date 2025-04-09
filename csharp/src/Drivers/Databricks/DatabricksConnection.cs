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
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    internal class DatabricksConnection : SparkHttpConnection
    {
        // CloudFetch configuration
        private bool _useCloudFetch = true;
        private bool _canDecompressLz4 = true;
        private long _maxBytesPerFile = 20 * 1024 * 1024; // 20MB default

        public DatabricksConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
            // Parse CloudFetch options from connection properties
            if (properties.TryGetValue(DatabricksParameters.UseCloudFetch, out string? useCloudFetchStr) &&
                bool.TryParse(useCloudFetchStr, out bool useCloudFetchValue))
            {
                _useCloudFetch = useCloudFetchValue;
            }

            if (properties.TryGetValue(DatabricksParameters.CanDecompressLz4, out string? canDecompressLz4Str) &&
                bool.TryParse(canDecompressLz4Str, out bool canDecompressLz4Value))
            {
                _canDecompressLz4 = canDecompressLz4Value;
            }

            if (properties.TryGetValue(DatabricksParameters.MaxBytesPerFile, out string? maxBytesPerFileStr) &&
                long.TryParse(maxBytesPerFileStr, out long maxBytesPerFileValue))
            {
                _maxBytesPerFile = maxBytesPerFileValue;
            }
        }

        /// <summary>
        /// Gets whether CloudFetch is enabled.
        /// </summary>
        internal bool UseCloudFetch => _useCloudFetch;

        /// <summary>
        /// Gets whether LZ4 decompression is enabled.
        /// </summary>
        internal bool CanDecompressLz4 => _canDecompressLz4;

        /// <summary>
        /// Gets the maximum bytes per file for CloudFetch.
        /// </summary>
        internal long MaxBytesPerFile => _maxBytesPerFile;

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
                return new DatabricksReader(databricksStatement, schema);
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
