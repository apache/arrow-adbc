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
        internal static readonly string s_userAgent = $"{DriverName.Replace(" ", "")}/{ProductVersionDefault}";

        private const string ProductVersionDefault = "1.0.0";
        private const string DriverName = "ADBC Impala Driver";
        private const string ArrowVersion = "1.0.0";
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

        protected override string ProductVersion => _productVersion.Value;

        protected override string GetProductVersionDefault() => ProductVersionDefault;

        public override AdbcStatement CreateStatement()
        {
            return new ImpalaStatement(this);
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

        protected override void SetPrecisionScaleAndTypeName(
            short colType,
            string typeName,
            TableInfo? tableInfo,
            int columnSize,
            int decimalDigits)
        {
            tableInfo?.TypeName.Add(typeName);
            tableInfo?.Precision.Add(columnSize);
            tableInfo?.Scale.Add((short)decimalDigits);
            tableInfo?.BaseTypeName.Add(typeName);
        }

        protected override string InfoDriverName => DriverName;

        protected override string InfoDriverArrowVersion => ArrowVersion;

        protected override bool GetObjectsPatternsRequireLowerCase => true;

        protected override bool IsColumnSizeValidForDecimal => true;

        protected internal override int PositionRequiredOffset => 0;

        internal override SchemaParser SchemaParser { get; } = new HiveServer2SchemaParser();

        protected abstract void ValidateConnection();

        protected abstract void ValidateAuthentication();

        protected abstract void ValidateOptions();

        internal abstract ImpalaServerType ServerType { get; }

        protected override ColumnsMetadataColumnNames GetColumnsMetadataColumnNames()
        {
            return new ColumnsMetadataColumnNames()
            {
                TableCatalog = TableCat,
                TableSchema = TableMd,
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
    }
}
