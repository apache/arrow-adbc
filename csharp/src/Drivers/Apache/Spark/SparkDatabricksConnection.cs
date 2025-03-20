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

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkDatabricksConnection : SparkHttpConnection
    {
        // Track the result format and compression for the current query
        private TSparkRowSetType currentResultFormat = TSparkRowSetType.ARROW_BASED_SET;
        private bool isLz4Compressed = false;

        public SparkDatabricksConnection(IReadOnlyDictionary<string, string> properties) : base(properties)
        {
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema)
        {
            // Choose the appropriate reader based on the result format
            if (currentResultFormat == TSparkRowSetType.URL_BASED_SET)
            {
                return new SparkCloudFetchReader(statement as HiveServer2Statement, schema, isLz4Compressed);
            }
            else
            {
                return new SparkDatabricksReader(statement as HiveServer2Statement, schema);
            }
        }

        internal override SchemaParser SchemaParser => new SparkDatabricksSchemaParser();

        internal override SparkServerType ServerType => SparkServerType.Databricks;

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

        /// <summary>
        /// Process the result set metadata to determine the result format and compression.
        /// </summary>
        /// <param name="metadata">The result set metadata.</param>
        internal void ProcessResultSetMetadata(TGetResultSetMetadataResp metadata)
        {
            // Check if the result format is specified
            if (metadata.__isset.resultFormat)
            {
                currentResultFormat = metadata.ResultFormat;
            }
            else
            {
                currentResultFormat = TSparkRowSetType.ARROW_BASED_SET;
            }

            // Check if the results are LZ4 compressed
            isLz4Compressed = metadata.__isset.lz4Compressed && metadata.Lz4Compressed;
        }

        // Add method to process metadata from execute statement response
        internal async Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TExecuteStatementResp response, CancellationToken cancellationToken = default)
        {
            var metadata = await HiveServer2Connection.GetResultSetMetadataAsync(response.OperationHandle, Client, cancellationToken);
            ProcessResultSetMetadata(metadata);
            return metadata;
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response, CancellationToken cancellationToken = default) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response, CancellationToken cancellationToken = default) =>
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
    }
}
