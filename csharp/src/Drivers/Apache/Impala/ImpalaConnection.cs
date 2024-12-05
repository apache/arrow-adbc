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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Ipc;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal abstract class ImpalaConnection : HiveServer2Connection
    {
        internal static readonly string s_userAgent = $"{InfoDriverName.Replace(" ", "")}/{ProductVersionDefault}";

        const string ProductVersionDefault = "1.0.0";
        const string InfoDriverName = "ADBC Impala Driver";

        private readonly Lazy<string> _productVersion;

        /*
        // https://impala.apache.org/docs/build/html/topics/impala_ports.html
        // https://impala.apache.org/docs/build/html/topics/impala_client.html
        private const int DefaultSocketTransportPort = 21050;
        private const int DefaultHttpTransportPort = 28000;
        */

        internal ImpalaConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
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

        protected string ProductVersion => _productVersion.Value;

        protected override string GetProductVersionDefault() => ProductVersionDefault;

        public override AdbcStatement CreateStatement()
        {
            return new ImpalaStatement(this);
        }

        // Note Impala's Position is one-indexed, not zero-indexed
        protected override IReadOnlyDictionary<string, int> GetColumnIndexMap(List<TColumnDesc> columns) => columns
           .Select(t => new { Index = t.Position, t.ColumnName })
           .ToDictionary(t => t.ColumnName, t => t.Index);

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

        protected override bool AreResultsAvailableDirectly() => false;

        protected override TSparkGetDirectResults GetDirectResults() => throw new System.NotImplementedException();

        public override IArrowArrayStream GetTableTypes() => throw new System.NotImplementedException();

        internal override SchemaParser SchemaParser { get; } = new HiveServer2SchemaParser();

        protected abstract void ValidateConnection();

        protected abstract void ValidateAuthentication();

        protected abstract void ValidateOptions();

        internal abstract ImpalaServerType ServerType { get; }
    }
}
