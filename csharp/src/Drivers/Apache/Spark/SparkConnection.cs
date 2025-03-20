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
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift.Transport;

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

        internal static readonly Dictionary<string, string> timestampConfig = new Dictionary<string, string>
        {
            { "spark.thriftserver.arrowBasedRowSet.timestampAsString", "false" }
        };

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

        protected internal override int PositionRequiredOffset => 1;

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

        internal override IArrowArrayStream NewReader<T>(T statement, Schema schema)
        {
            // Check if the statement is a SparkStatement
            if (statement is SparkStatement sparkStatement)
            {
                try
                {
                    // Check if we have a TGetResultSetMetadataResp with URL_BASED_SET type
                    TGetResultSetMetadataResp? resultSetMetadata = GetResultSetMetadataAsync(
                        sparkStatement.OperationHandle!, 
                        Client, 
                        CancellationToken.None).Result;
                    
                    // Check if we have URL-based results (CloudFetch) and CloudFetch is enabled
                    if (sparkStatement.UseCloudFetch && 
                        resultSetMetadata.__isset.resultFormat && 
                        resultSetMetadata.ResultFormat == TSparkRowSetType.URL_BASED_SET)
                    {
                        // Create a CloudFetch reader
                        bool isLz4Compressed = resultSetMetadata.__isset.lz4Compressed && resultSetMetadata.Lz4Compressed && sparkStatement.CanDecompressLz4;
                        return new CloudFetch.SparkCloudFetchReader(statement, schema, isLz4Compressed);
                    }
                    
                    // Check if we have Arrow-based results
                    if (resultSetMetadata.__isset.resultFormat && resultSetMetadata.ResultFormat == TSparkRowSetType.ARROW_BASED_SET)
                    {
                        return new SparkDatabricksReader(statement, schema);
                    }
                }
                catch (Exception ex)
                {
                    // Log the error and fall back to the default reader
                    Console.WriteLine($"Error creating CloudFetch reader: {ex.Message}. Falling back to default reader.");
                }
            }
            
            // Fall back to the default reader
            return new HiveServer2Reader(statement, schema, DataTypeConversion);
        }

        protected override bool AreResultsAvailableDirectly() => true;

        protected override void SetDirectResults(TGetColumnsReq request) => request.GetDirectResults = sparkGetDirectResults;

        protected override void SetDirectResults(TGetCatalogsReq request) => request.GetDirectResults = sparkGetDirectResults;

        protected override void SetDirectResults(TGetSchemasReq request) => request.GetDirectResults = sparkGetDirectResults;

        protected override void SetDirectResults(TGetTablesReq request) => request.GetDirectResults = sparkGetDirectResults;

        protected override void SetDirectResults(TGetTableTypesReq request) => request.GetDirectResults = sparkGetDirectResults;

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
    }
}
