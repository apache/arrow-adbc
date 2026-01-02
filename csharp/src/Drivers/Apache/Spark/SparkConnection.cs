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
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal abstract class SparkConnection : HiveServer2Connection
    {
        protected const string ProductVersionDefault = "1.0.0";
        protected const string DriverName = "ADBC Spark Driver";
        private const string ArrowVersion = "1.0.0";
        private readonly Lazy<string> _productVersion;

        protected override string GetProductVersionDefault() => ProductVersionDefault;

        internal static TSparkGetDirectResults sparkGetDirectResults = new TSparkGetDirectResults(1000);

        internal SparkConnection(IReadOnlyDictionary<string, string> properties)
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

        public override AdbcStatement CreateStatement()
        {
            SparkStatement statement = new SparkStatement(this);
            return statement;
        }

        protected override Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(IResponse response, CancellationToken cancellationToken = default) =>
            GetResultSetMetadataAsync(response.OperationHandle!, Client, cancellationToken);
        protected override Task<TRowSet> GetRowSetAsync(IResponse response, CancellationToken cancellationToken = default) =>
            FetchResultsAsync(response.OperationHandle!, cancellationToken: cancellationToken);

        protected internal override int PositionRequiredOffset => 1;

        protected override int ColumnMapIndexOffset => 1;

        internal override void SetPrecisionScaleAndTypeName(
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
                        SqlDecimalParserResult result = SqlTypeNameParser<SqlDecimalParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(result.Precision);
                        tableInfo?.Scale.Add((short)result.Scale);
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
                        SqlCharVarcharParserResult result = SqlTypeNameParser<SqlCharVarcharParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(result.ColumnSize);
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

        protected override string InfoDriverName => DriverName;

        protected override string InfoDriverArrowVersion => ArrowVersion;

        protected override bool GetObjectsPatternsRequireLowerCase => false;

        protected override bool IsColumnSizeValidForDecimal => false;

        internal override SchemaParser SchemaParser { get; } = new HiveServer2SchemaParser();

        protected internal override bool TrySetGetDirectResults(IRequest request)
        {
            request.GetDirectResults = sparkGetDirectResults;
            return true;
        }

        protected internal override bool TryGetDirectResults(TSparkDirectResults? directResults, [MaybeNullWhen(false)] out QueryResult result)
        {
            if (directResults?.ResultSet?.Results == null)
            {
                result = null;
                return false;
            }

            TGetResultSetMetadataResp resultSetMetadata = directResults.ResultSetMetadata;
            Schema schema = SchemaParser.GetArrowSchema(resultSetMetadata.Schema, DataTypeConversion);
            TRowSet rowSet = directResults.ResultSet.Results;
            int columnCount = HiveServer2Reader.GetColumnCount(rowSet);
            int rowCount = HiveServer2Reader.GetRowCount(rowSet, columnCount);
            IReadOnlyList<IArrowArray> data = HiveServer2Reader.GetArrowArrayData(rowSet, columnCount, schema, DataTypeConversion);

            result = new QueryResult(rowCount, new HiveServer2Connection.HiveInfoArrowStream(schema, data));
            return true;
        }

        protected internal override bool TryGetDirectResults(
            TSparkDirectResults? directResults,
            [MaybeNullWhen(false)] out TGetResultSetMetadataResp metadata,
            [MaybeNullWhen(false)] out TRowSet rowSet)
        {
            if (directResults?.ResultSet?.Results == null)
            {
                metadata = null;
                rowSet = null;
                return false;
            }

            metadata = directResults.ResultSetMetadata;
            rowSet = directResults.ResultSet.Results;
            return true;
        }

        protected abstract void ValidateConnection();
        protected abstract void ValidateAuthentication();
        protected abstract void ValidateOptions();

        internal abstract SparkServerType ServerType { get; }

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

        protected static new class ActivityKeys
        {
            private const string Prefix = "spark.";

            public const string Encrypted = Prefix + Apache.ActivityKeys.Encrypted;
            public const string TransportType = Prefix + Apache.ActivityKeys.TransportType;
            public const string Host = Prefix + Apache.ActivityKeys.Host;
            public const string Port = Prefix + Apache.ActivityKeys.Port;
            public const string AuthType = Prefix + Apache.ActivityKeys.AuthType;

            public static class Http
            {
                public const string UserAgent = Prefix + Apache.ActivityKeys.Http.UserAgent;
                public const string Uri = Prefix + Apache.ActivityKeys.Http.Uri;
                public const string AuthScheme = Prefix + Apache.ActivityKeys.Http.AuthScheme;
            }
        }
    }
}
