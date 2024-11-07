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

using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal class SparkDatabricksConnection : SparkHttpConnection
    {
        public SparkDatabricksConnection(IReadOnlyDictionary<string, string> properties, ActivitySource? activitySource) : base(properties, activitySource)
        {
        }

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, CancellationToken cancellationToken = default) => new SparkDatabricksReader(statement, schema);

        internal override SchemaParser SchemaParser => new SparkDatabricksSchemaParser();

        internal override SparkServerType ServerType => SparkServerType.Databricks;

        protected override TOpenSessionReq CreateSessionRequest()
        {
            var req = new TOpenSessionReq(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7)
            {
                CanUseMultipleCatalogs = true,
            };
            return req;
        }

        protected override void ValidateOptions()
        {
            Properties.TryGetValue(SparkParameters.DataTypeConv, out string? dataTypeConv);
            // Note: In Databricks, scalar types are provided implicitly.
            DataTypeConversion = DataTypeConversionParser.Parse(dataTypeConv);

            Properties.TryGetValue(SparkParameters.TLSOptions, out string? tlsOptions);
            TlsOptions = TlsOptionsParser.Parse(tlsOptions);
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);
        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response) =>
            Task.FromResult(response.DirectResults.ResultSetMetadata);

        protected override Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetColumnsResp response) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetTablesResp response) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
        protected override Task<TRowSet> GetRowSetAsync(TGetSchemasResp response) =>
            Task.FromResult(response.DirectResults.ResultSet.Results);
    }
}
