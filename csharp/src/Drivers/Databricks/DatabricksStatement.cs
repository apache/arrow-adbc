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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Databricks-specific implementation of <see cref="AdbcStatement"/>
    /// </summary>
    internal class DatabricksStatement : SparkStatement, IHiveServer2Statement
    {
        private bool useCloudFetch;
        private bool canDecompressLz4;
        private long maxBytesPerFile;
        private bool enableMultipleCatalogSupport;

        public DatabricksStatement(DatabricksConnection connection)
            : base(connection)
        {
            // Inherit CloudFetch settings from connection
            useCloudFetch = connection.UseCloudFetch;
            canDecompressLz4 = connection.CanDecompressLz4;
            maxBytesPerFile = connection.MaxBytesPerFile;
            enableMultipleCatalogSupport = connection.EnableMultipleCatalogSupport;
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            base.SetStatementProperties(statement);

            // Set CloudFetch capabilities
            statement.CanDownloadResult = useCloudFetch;
            statement.CanDecompressLZ4Result = canDecompressLz4;
            statement.MaxBytesPerFile = maxBytesPerFile;

            if (Connection.AreResultsAvailableDirectly)
            {
                statement.GetDirectResults = DatabricksConnection.defaultGetDirectResults;
            }
        }

        /// <summary>
        /// Checks if direct results are available.
        /// </summary>
        /// <returns>True if direct results are available and contain result data, false otherwise.</returns>
        public bool HasDirectResults => DirectResults?.ResultSet != null && DirectResults?.ResultSetMetadata != null;

        public TSparkDirectResults? DirectResults
        {
            get { return _directResults; }
        }

        // Cast the Client to IAsync for CloudFetch compatibility
        TCLIService.IAsync IHiveServer2Statement.Client => Connection.Client;

        public override void SetOption(string key, string value)
        {
            switch (key)
            {
                case DatabricksParameters.UseCloudFetch:
                    if (bool.TryParse(value, out bool useCloudFetchValue))
                    {
                        this.useCloudFetch = useCloudFetchValue;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a boolean value.");
                    }
                    break;
                case DatabricksParameters.CanDecompressLz4:
                    if (bool.TryParse(value, out bool canDecompressLz4Value))
                    {
                        this.canDecompressLz4 = canDecompressLz4Value;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a boolean value.");
                    }
                    break;
                case DatabricksParameters.MaxBytesPerFile:
                    if (long.TryParse(value, out long maxBytesPerFileValue))
                    {
                        this.maxBytesPerFile = maxBytesPerFileValue;
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a long value.");
                    }
                    break;
                default:
                    base.SetOption(key, value);
                    break;
            }
        }

        /// <summary>
        /// Sets whether to use CloudFetch for retrieving results.
        /// </summary>
        /// <param name="useCloudFetch">Whether to use CloudFetch.</param>
        internal void SetUseCloudFetch(bool useCloudFetch)
        {
            this.useCloudFetch = useCloudFetch;
        }

        /// <summary>
        /// Gets whether CloudFetch is enabled.
        /// </summary>
        public bool UseCloudFetch => useCloudFetch;

        /// <summary>
        /// Gets the maximum bytes per file for CloudFetch.
        /// </summary>
        public long MaxBytesPerFile => maxBytesPerFile;

        /// <summary>
        /// Gets whether LZ4 decompression is enabled.
        /// </summary>
        public bool CanDecompressLz4 => canDecompressLz4;

        /// <summary>
        /// Sets whether the client can decompress LZ4 compressed results.
        /// </summary>
        /// <param name="canDecompressLz4">Whether the client can decompress LZ4.</param>
        internal void SetCanDecompressLz4(bool canDecompressLz4)
        {
            this.canDecompressLz4 = canDecompressLz4;
        }

        /// <summary>
        /// Sets the maximum bytes per file for CloudFetch.
        /// </summary>
        /// <param name="maxBytesPerFile">The maximum bytes per file.</param>
        internal void SetMaxBytesPerFile(long maxBytesPerFile)
        {
            this.maxBytesPerFile = maxBytesPerFile;
        }

        /// <summary>
        /// Helper method to handle the special case for the "SPARK" catalog in metadata queries.
        ///
        /// Why:
        /// - In Databricks, the legacy "SPARK" catalog is used as a placeholder to represent the default catalog.
        /// - When a client requests metadata for the "SPARK" catalog, the underlying API expects a null catalog name
        ///   to trigger default catalog behavior. Passing "SPARK" directly would not return the expected results.
        ///
        /// What it does:
        /// - If the CatalogName property is set to "SPARK" (case-insensitive), this method sets it to null.
        /// - This ensures that downstream API calls behave as if no catalog was specified, returning default catalog metadata.
        ///
        /// This logic is required to maintain compatibility with legacy tools and standards that expect "SPARK" to act as a default catalog alias.
        /// </summary>
        private void HandleSparkCatalog()
        {
            if (CatalogName != null && CatalogName.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
            {
                CatalogName = null;
            }
        }

        /// <summary>
        /// Overrides the GetCatalogsAsync method to handle EnableMultipleCatalogSupport flag.
        /// When EnableMultipleCatalogSupport is false, returns a single catalog "SPARK" without making an RPC call.
        /// When EnableMultipleCatalogSupport is true, delegates to the base class implementation to retrieve actual catalogs.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Query result containing catalog information</returns>
        protected override async Task<QueryResult> GetCatalogsAsync(CancellationToken cancellationToken = default)
        {
            // If EnableMultipleCatalogSupport is false, return a single catalog "SPARK" without making an RPC call
            if (!enableMultipleCatalogSupport)
            {
                // Create a schema with a single column TABLE_CAT
                var field = new Field("TABLE_CAT", StringType.Default, true);
                var schema = new Schema(new[] { field }, null);

                // Create a single row with value "SPARK"
                var builder = new StringArray.Builder();
                builder.Append("SPARK");
                var array = builder.Build();

                // Return the result without making an RPC call
                return new QueryResult(1, new HiveServer2Connection.HiveInfoArrowStream(schema, new[] { array }));
            }

            // If EnableMultipleCatalogSupport is true, delegate to base class implementation
            return await base.GetCatalogsAsync(cancellationToken);
        }

        /// <summary>
        /// Overrides the GetSchemasAsync method to handle the SPARK catalog case.
        /// When EnableMultipleCatalogSupport is true:
        ///   - If catalog is "SPARK", sets catalogName to null in the API call
        /// When EnableMultipleCatalogSupport is false:
        ///   - If catalog is not null or SPARK, returns empty result without RPC call
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Query result containing schema information</returns>
        protected override async Task<QueryResult> GetSchemasAsync(CancellationToken cancellationToken = default)
        {
            // Handle SPARK catalog case
            HandleSparkCatalog();

            // If EnableMultipleCatalogSupport is false and catalog is not null or SPARK, return empty result without RPC call
            if (!enableMultipleCatalogSupport && CatalogName != null)
            {
                // Create a schema with TABLE_SCHEM and TABLE_CATALOG columns
                var fields = new[]
                {
                    new Field("TABLE_SCHEM", StringType.Default, true),
                    new Field("TABLE_CATALOG", StringType.Default, true)
                };
                var schema = new Schema(fields, null);

                // Create empty arrays for both columns
                var catalogArray = new StringArray.Builder().Build();
                var schemaArray = new StringArray.Builder().Build();

                // Return empty result
                return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, new[] { catalogArray, schemaArray }));
            }

            // Call the base implementation with the potentially modified catalog name
            return await base.GetSchemasAsync(cancellationToken);
        }

        /// <summary>
        /// Overrides the GetTablesAsync method to handle the SPARK catalog case.
        /// When EnableMultipleCatalogSupport is true:
        ///   - If catalog is "SPARK", sets catalogName to null in the API call
        /// When EnableMultipleCatalogSupport is false:
        ///   - If catalog is not null or SPARK, returns empty result without RPC call
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Query result containing table information</returns>
        protected override async Task<QueryResult> GetTablesAsync(CancellationToken cancellationToken = default)
        {
            // Handle SPARK catalog case
            HandleSparkCatalog();

            // If EnableMultipleCatalogSupport is false and catalog is not null or SPARK, return empty result without RPC call
            if (!enableMultipleCatalogSupport && CatalogName != null)
            {
                // Correct schema for GetTables
                var fields = new[]
                {
                    new Field("TABLE_CAT", StringType.Default, true),
                    new Field("TABLE_SCHEM", StringType.Default, true),
                    new Field("TABLE_NAME", StringType.Default, true),
                    new Field("TABLE_TYPE", StringType.Default, true),
                    new Field("REMARKS", StringType.Default, true),
                    new Field("TYPE_CAT", StringType.Default, true),
                    new Field("TYPE_SCHEM", StringType.Default, true),
                    new Field("TYPE_NAME", StringType.Default, true),
                    new Field("SELF_REFERENCING_COL_NAME", StringType.Default, true),
                    new Field("REF_GENERATION", StringType.Default, true)
                };
                var schema = new Schema(fields, null);

                // Create empty arrays for all columns
                var arrays = new IArrowArray[]
                {
                    new StringArray.Builder().Build(), // TABLE_CAT
                    new StringArray.Builder().Build(), // TABLE_SCHEM
                    new StringArray.Builder().Build(), // TABLE_NAME
                    new StringArray.Builder().Build(), // TABLE_TYPE
                    new StringArray.Builder().Build(), // REMARKS
                    new StringArray.Builder().Build(), // TYPE_CAT
                    new StringArray.Builder().Build(), // TYPE_SCHEM
                    new StringArray.Builder().Build(), // TYPE_NAME
                    new StringArray.Builder().Build(), // SELF_REFERENCING_COL_NAME
                    new StringArray.Builder().Build()  // REF_GENERATION
                };

                // Return empty result
                return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, arrays));
            }

            // Call the base implementation with the potentially modified catalog name
            return await base.GetTablesAsync(cancellationToken);
        }

        /// <summary>
        /// Overrides the GetColumnsAsync method to handle the SPARK catalog case.
        /// When EnableMultipleCatalogSupport is true:
        ///   - If catalog is "SPARK", sets catalogName to null in the API call
        /// When EnableMultipleCatalogSupport is false:
        ///   - If catalog is not null or SPARK, returns empty result without RPC call
        /// </summary>
        /// <param name="cancellationToken">Cancellation token</param>
        /// <returns>Query result containing column information</returns>
        protected override async Task<QueryResult> GetColumnsAsync(CancellationToken cancellationToken = default)
        {
            // Handle SPARK catalog case
            HandleSparkCatalog();

            // If EnableMultipleCatalogSupport is false and catalog is not null, return empty result without RPC call
            if (!enableMultipleCatalogSupport && CatalogName != null)
            {
                // Correct schema for GetColumns
                var fields = new[]
                {
                    new Field("TABLE_CAT", StringType.Default, true),
                    new Field("TABLE_SCHEM", StringType.Default, true),
                    new Field("TABLE_NAME", StringType.Default, true),
                    new Field("COLUMN_NAME", StringType.Default, true),
                    new Field("DATA_TYPE", Int32Type.Default, true),
                    new Field("TYPE_NAME", StringType.Default, true),
                    new Field("COLUMN_SIZE", Int32Type.Default, true),
                    new Field("BUFFER_LENGTH", Int8Type.Default, true),
                    new Field("DECIMAL_DIGITS", Int32Type.Default, true),
                    new Field("NUM_PREC_RADIX", Int32Type.Default, true),
                    new Field("NULLABLE", Int32Type.Default, true),
                    new Field("REMARKS", StringType.Default, true),
                    new Field("COLUMN_DEF", StringType.Default, true),
                    new Field("SQL_DATA_TYPE", Int32Type.Default, true),
                    new Field("SQL_DATETIME_SUB", Int32Type.Default, true),
                    new Field("CHAR_OCTET_LENGTH", Int32Type.Default, true),
                    new Field("ORDINAL_POSITION", Int32Type.Default, true),
                    new Field("IS_NULLABLE", StringType.Default, true),
                    new Field("SCOPE_CATALOG", StringType.Default, true),
                    new Field("SCOPE_SCHEMA", StringType.Default, true),
                    new Field("SCOPE_TABLE", StringType.Default, true),
                    new Field("SOURCE_DATA_TYPE", Int16Type.Default, true),
                    new Field("IS_AUTO_INCREMENT", StringType.Default, true),
                    new Field("BASE_TYPE_NAME", StringType.Default, true)
                };
                var schema = new Schema(fields, null);

                // Create empty arrays for all columns
                var arrays = new IArrowArray[]
                {
                    new StringArray.Builder().Build(), // TABLE_CAT
                    new StringArray.Builder().Build(), // TABLE_SCHEM
                    new StringArray.Builder().Build(), // TABLE_NAME
                    new StringArray.Builder().Build(), // COLUMN_NAME
                    new Int32Array.Builder().Build(),  // DATA_TYPE
                    new StringArray.Builder().Build(), // TYPE_NAME
                    new Int32Array.Builder().Build(),  // COLUMN_SIZE
                    new Int8Array.Builder().Build(),   // BUFFER_LENGTH
                    new Int32Array.Builder().Build(),  // DECIMAL_DIGITS
                    new Int32Array.Builder().Build(),  // NUM_PREC_RADIX
                    new Int32Array.Builder().Build(),  // NULLABLE
                    new StringArray.Builder().Build(), // REMARKS
                    new StringArray.Builder().Build(), // COLUMN_DEF
                    new Int32Array.Builder().Build(),  // SQL_DATA_TYPE
                    new Int32Array.Builder().Build(),  // SQL_DATETIME_SUB
                    new Int32Array.Builder().Build(),  // CHAR_OCTET_LENGTH
                    new Int32Array.Builder().Build(),  // ORDINAL_POSITION
                    new StringArray.Builder().Build(), // IS_NULLABLE
                    new StringArray.Builder().Build(), // SCOPE_CATALOG
                    new StringArray.Builder().Build(), // SCOPE_SCHEMA
                    new StringArray.Builder().Build(), // SCOPE_TABLE
                    new Int16Array.Builder().Build(),  // SOURCE_DATA_TYPE
                    new StringArray.Builder().Build(), // IS_AUTO_INCREMENT
                    new StringArray.Builder().Build()  // BASE_TYPE_NAME
                };

                // Return empty result
                return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, arrays));
            }

            // Call the base implementation with the potentially modified catalog name
            return await base.GetColumnsAsync(cancellationToken);
        }
    }
}
