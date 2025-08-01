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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks.CloudFetch;
using Apache.Arrow.Adbc.Drivers.Databricks.Result;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using static Apache.Arrow.Adbc.Drivers.Databricks.Result.DescTableExtendedResult;

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
        private bool enablePKFK;
        private bool runAsyncInThrift;

        public DatabricksStatement(DatabricksConnection connection)
            : base(connection)
        {
            // set the catalog name for legacy compatibility
            // TODO: use catalog and schema fields in hiveserver2 connection instead of DefaultNamespace so we don't need to cast
            var defaultNamespace = ((DatabricksConnection)Connection).DefaultNamespace;
            if (defaultNamespace != null)
            {
                // TODO: we should not blindly overwrite, for crossReferenceAsync handling (though, still works)
                if (CatalogName == null && connection.EnableMultipleCatalogSupport)
                {
                    CatalogName = defaultNamespace.CatalogName;
                }
            }
            // Inherit CloudFetch settings from connection
            useCloudFetch = connection.UseCloudFetch;
            canDecompressLz4 = connection.CanDecompressLz4;
            maxBytesPerFile = connection.MaxBytesPerFile;
            enableMultipleCatalogSupport = connection.EnableMultipleCatalogSupport;
            enablePKFK = connection.EnablePKFK;

            runAsyncInThrift = connection.RunAsyncInThrift;
        }

        /// <summary>
        /// Gets the schema from metadata response. Handles both Arrow schema (Protocol V5+) and traditional Thrift schema.
        /// </summary>
        /// <param name="metadata">The metadata response containing schema information</param>
        /// <returns>The Arrow schema</returns>
        protected override Schema GetSchemaFromMetadata(TGetResultSetMetadataResp metadata)
        {
            // For Protocol V5+, prefer Arrow schema if available
            if (metadata.__isset.arrowSchema)
            {
                Schema? arrowSchema = ((DatabricksSchemaParser)Connection.SchemaParser).ParseArrowSchema(metadata.ArrowSchema);
                if (arrowSchema != null)
                {
                    return arrowSchema;
                }
            }

            // Fallback to traditional Thrift schema
            return Connection.SchemaParser.GetArrowSchema(metadata.Schema, Connection.DataTypeConversion);
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            base.SetStatementProperties(statement);

            // Set Databricks-specific statement properties
            // TODO: Ensure this is set dynamically depending on server capabilities.
            statement.EnforceResultPersistenceMode = false;
            statement.ResultPersistenceMode = TResultPersistenceMode.ALL_RESULTS;
            statement.CanReadArrowResult = true;

#pragma warning disable CS0618 // Type or member is obsolete
            statement.ConfOverlay = SparkConnection.timestampConfig;
#pragma warning restore CS0618 // Type or member is obsolete

            statement.UseArrowNativeTypes = new TSparkArrowTypes
            {
                TimestampAsArrow = true,
                DecimalAsArrow = true,

                // set to false so they return as string
                // otherwise, they return as ARRAY_TYPE but you can't determine
                // the object type of the items in the array
                ComplexTypesAsArrow = false,
                IntervalTypesAsArrow = false,
            };

            // Set CloudFetch capabilities
            statement.CanDownloadResult = useCloudFetch;
            statement.CanDecompressLZ4Result = canDecompressLz4;
            statement.MaxBytesPerFile = maxBytesPerFile;
            statement.RunAsync = runAsyncInThrift;

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
            CatalogName = DatabricksConnection.HandleSparkCatalog(CatalogName);
        }

        /// <summary>
        /// Helper method that returns the fully qualified table name enclosed by backtick.
        /// The returned value can be used as table name in the SQL statement
        ///
        /// If only SchemaName is defined, it will return `SchemaName`.`TableName`
        /// If both CatalogName and SchemaName are defined, it will return `CatalogName`.`SchenaName`.`TableName`
        /// </summary>
        protected string? BuildTableName()
        {
            if (string.IsNullOrEmpty(TableName))
            {
                return TableName;
            }

            var parts = new List<string>();

            if (!string.IsNullOrEmpty(SchemaName))
            {
                // Only include CatalogName when SchemaName is defined
                if (!string.IsNullOrEmpty(CatalogName) && !CatalogName!.Equals("SPARK", StringComparison.OrdinalIgnoreCase))
                {
                    parts.Add($"`{CatalogName.Replace("`", "``")}`");
                }
                parts.Add($"`{SchemaName!.Replace("`", "``")}`");
            }

            // Escape if TableName contains backtick
            parts.Add($"`{TableName!.Replace("`", "``")}`");

            return string.Join(".", parts);
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
                var schema = CreateColumnMetadataSchema();


                // Create empty arrays for all columns
                var arrays = CreateColumnMetadataEmptyArray();

                // Return empty result
                return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, arrays));
            }

            // Call the base implementation with the potentially modified catalog name
            return await base.GetColumnsAsync(cancellationToken);
        }

        /// <summary>
        /// Determines whether PK/FK metadata queries (GetPrimaryKeys/GetCrossReference) should return an empty result set without hitting the server.
        ///
        /// Why:
        /// - For certain catalog names (null, empty, "SPARK", "hive_metastore"), Databricks does not support PK/FK metadata,
        ///   or these are legacy/synthesized catalogs that should gracefully return empty results for compatibility.
        /// - The EnablePKFK flag allows the client to globally disable PK/FK metadata queries for performance or compatibility reasons.
        ///
        /// What it does:
        /// - Returns true if PK/FK queries should return an empty result (and not hit the server), based on:
        ///   - The EnablePKFK flag (if false, always return empty)
        ///   - The catalog name (SPARK, hive_metastore, null, or empty string)
        /// - Returns false if the query should proceed to the server (for valid, supported catalogs).
        /// </summary>
        internal bool ShouldReturnEmptyPkFkResult()
        {
            if (!enablePKFK)
                return true;

            var catalogInvalid = string.IsNullOrEmpty(CatalogName) ||
                string.Equals(CatalogName, "SPARK", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(CatalogName, "hive_metastore", StringComparison.OrdinalIgnoreCase);

            var foreignCatalogInvalid = string.IsNullOrEmpty(ForeignCatalogName) ||
                string.Equals(ForeignCatalogName, "SPARK", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(ForeignCatalogName, "hive_metastore", StringComparison.OrdinalIgnoreCase);

            // Handle special catalog cases
            // Only when both catalog and foreignCatalog is Invalid, we return empty results
            if (catalogInvalid && foreignCatalogInvalid)
            {
                return true;
            }

            return false;
        }

        protected override async Task<QueryResult> GetPrimaryKeysAsync(CancellationToken cancellationToken = default)
        {
            if (ShouldReturnEmptyPkFkResult())
                return EmptyPrimaryKeysResult();

            return await base.GetPrimaryKeysAsync(cancellationToken);
        }

        private QueryResult EmptyPrimaryKeysResult()
        {
            var fields = new[]
            {
                new Field("TABLE_CAT", StringType.Default, true),
                new Field("TABLE_SCHEM", StringType.Default, true),
                new Field("TABLE_NAME", StringType.Default, true),
                new Field("COLUMN_NAME", StringType.Default, true),
                new Field("KEQ_SEQ", Int32Type.Default, true),
                new Field("PK_NAME", StringType.Default, true)
            };
            var schema = new Schema(fields, null);

            var arrays = new IArrowArray[]
            {
                new StringArray.Builder().Build(), // TABLE_CAT
                new StringArray.Builder().Build(), // TABLE_SCHEM
                new StringArray.Builder().Build(), // TABLE_NAME
                new StringArray.Builder().Build(), // COLUMN_NAME
                new Int32Array.Builder().Build(),  // KEQ_SEQ
                new StringArray.Builder().Build()  // PK_NAME
            };

            return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, arrays));
        }

        protected override async Task<QueryResult> GetCrossReferenceAsync(CancellationToken cancellationToken = default)
        {
            if (ShouldReturnEmptyPkFkResult())
                return EmptyCrossReferenceResult();

            return await base.GetCrossReferenceAsync(cancellationToken);
        }

        protected override async Task<QueryResult> GetCrossReferenceAsForeignTableAsync(CancellationToken cancellationToken = default)
        {
            if (ShouldReturnEmptyPkFkResult())
                return EmptyCrossReferenceResult();

            return await base.GetCrossReferenceAsForeignTableAsync(cancellationToken);
        }

        private QueryResult EmptyCrossReferenceResult()
        {
            var fields = new[]
            {
                new Field("PKTABLE_CAT", StringType.Default, true),
                new Field("PKTABLE_SCHEM", StringType.Default, true),
                new Field("PKTABLE_NAME", StringType.Default, true),
                new Field("PKCOLUMN_NAME", StringType.Default, true),
                new Field("FKTABLE_CAT", StringType.Default, true),
                new Field("FKTABLE_SCHEM", StringType.Default, true),
                new Field("FKTABLE_NAME", StringType.Default, true),
                new Field("FKCOLUMN_NAME", StringType.Default, true),
                new Field("KEQ_SEQ", Int32Type.Default, true),
                new Field("UPDATE_RULE", Int32Type.Default, true),
                new Field("DELETE_RULE", Int32Type.Default, true),
                new Field("FK_NAME", StringType.Default, true),
                new Field("PK_NAME", StringType.Default, true),
                new Field("DEFERRABILITY", Int32Type.Default, true)
            };
            var schema = new Schema(fields, null);

            var arrays = new IArrowArray[]
            {
                new StringArray.Builder().Build(), // PKTABLE_CAT
                new StringArray.Builder().Build(), // PKTABLE_SCHEM
                new StringArray.Builder().Build(), // PKTABLE_NAME
                new StringArray.Builder().Build(), // PKCOLUMN_NAME
                new StringArray.Builder().Build(), // FKTABLE_CAT
                new StringArray.Builder().Build(), // FKTABLE_SCHEM
                new StringArray.Builder().Build(), // FKTABLE_NAME
                new StringArray.Builder().Build(), // FKCOLUMN_NAME
                new Int32Array.Builder().Build(),  // KEQ_SEQ
                new Int32Array.Builder().Build(),  // UPDATE_RULE
                new Int32Array.Builder().Build(),  // DELETE_RULE
                new StringArray.Builder().Build(), // FK_NAME
                new StringArray.Builder().Build(), // PK_NAME
                new Int32Array.Builder().Build()   // DEFERRABILITY
            };

            return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, arrays));
        }

        protected override async Task<QueryResult> GetColumnsExtendedAsync(CancellationToken cancellationToken = default)
        {
            string? fullTableName = BuildTableName();
            var canUseDescTableExtended = ((DatabricksConnection)Connection).CanUseDescTableExtended;

            if (!canUseDescTableExtended || string.IsNullOrEmpty(fullTableName))
            {
                // When fullTableName is empty, we cannot use metadata SQL query to get the info,
                // so fallback to base class implementation
                return await base.GetColumnsExtendedAsync(cancellationToken);
            }

            string query = $"DESC TABLE EXTENDED {fullTableName} AS JSON";
            using var descStmt = Connection.CreateStatement();
            descStmt.SqlQuery = query;
            QueryResult descResult;

            try
            {
                descResult = await descStmt.ExecuteQueryAsync();
            }
            catch (HiveServer2Exception ex) when (ex.SqlState == "42601" || ex.SqlState == "20000")
            {
                // 42601 is error code of syntax error, which this command (DESC TABLE EXTENDED ... AS JSON) is not supported by current DBR
                // Sometimes server may also return 20000 (internal error) if it fails to convert some data types of the table columns
                // So we should fallback to base implementation
                Debug.WriteLine($"[WARN] Failed to run {query} (reason={ex.Message}). Fallback to base::GetColumnsExtendedAsync.");
                return await base.GetColumnsExtendedAsync(cancellationToken);
            }

            var columnMetadataSchema = CreateColumnMetadataSchema();

            if (descResult.Stream == null)
            {
                return CreateEmptyExtendedColumnsResult(columnMetadataSchema);
            }

            // Read the json result
            var resultJson = "";
            using (var stream = descResult.Stream)
            {
                var batch = await stream.ReadNextRecordBatchAsync(cancellationToken);
                if (batch == null || batch.Length == 0)
                {
                    return CreateEmptyExtendedColumnsResult(columnMetadataSchema);
                }

                resultJson = ((StringArray)batch.Column(0)).GetString(0);
            }

            // Parse the JSON result
            var result = JsonSerializer.Deserialize<DescTableExtendedResult>(resultJson);
            if (result == null)
            {
                throw new FormatException($"Invalid json result of {query}.Result={resultJson}");
            }
            return CreateExtendedColumnsResult(columnMetadataSchema,result);
        }

        public override string AssemblyName => DatabricksConnection.s_assemblyName;

        public override string AssemblyVersion => DatabricksConnection.s_assemblyVersion;

        /// <summary>
        /// Creates the schema for the column metadata result set.
        /// This schema is used for the GetColumns metadata query.
        /// </summary>
        private static Schema CreateColumnMetadataSchema()
        {
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
            return new Schema(fields, null);
        }

        /// <summary>
        /// Creates an empty array for each column in the column metadata schema.
        /// </summary>
        private static IArrowArray[] CreateColumnMetadataEmptyArray()
        {
            return
            [
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
            ];
        }

        private QueryResult CreateExtendedColumnsResult(Schema columnMetadataSchema, DescTableExtendedResult descResult)
        {
            var allFields = new List<Field>(columnMetadataSchema.FieldsList);
            foreach (var field in PrimaryKeyFields)
            {
                allFields.Add(new Field(PrimaryKeyPrefix + field, StringType.Default, true));
            }

            foreach (var field in ForeignKeyFields)
            {
                IArrowType fieldType = field != "KEQ_SEQ" ? StringType.Default : Int32Type.Default;
                allFields.Add(new Field(ForeignKeyPrefix + field, fieldType, true));
            }

            var combinedSchema = new Schema(allFields, columnMetadataSchema.Metadata);

            var tableCatBuilder = new StringArray.Builder();
            var tableSchemaBuilder = new StringArray.Builder();
            var tableNameBuilder = new StringArray.Builder();
            var columnNameBuilder = new StringArray.Builder();
            var dataTypeBuilder = new Int32Array.Builder();
            var typeNameBuilder = new StringArray.Builder();
            var columnSizeBuilder = new Int32Array.Builder();
            var bufferLengthBuilder = new Int8Array.Builder();
            var decimalDigitsBuilder = new Int32Array.Builder();
            var numPrecRadixBuilder = new Int32Array.Builder();
            var nullableBuilder = new Int32Array.Builder();
            var remarksBuilder = new StringArray.Builder();
            var columnDefBuilder = new StringArray.Builder();
            var sqlDataTypeBuilder = new Int32Array.Builder();
            var sqlDatetimeSubBuilder = new Int32Array.Builder();
            var charOctetLengthBuilder = new Int32Array.Builder();
            var ordinalPositionBuilder = new Int32Array.Builder();
            var isNullableBuilder = new StringArray.Builder();
            var scopeCatalogBuilder = new StringArray.Builder();
            var scopeSchemaBuilder = new StringArray.Builder();
            var scopeTableBuilder = new StringArray.Builder();
            var sourceDataTypeBuilder = new Int16Array.Builder();
            var isAutoIncrementBuilder = new StringArray.Builder();
            var baseTypeNameBuilder = new StringArray.Builder();

            // PK_COLUMN_NAME: Metadata column for primary key
            var pkColumnBuilder = new StringArray.Builder();

            // Metadata columns for foreign key info
            var fkColumnLocalBuilder = new StringArray.Builder();
            var fkColumnRefCatalogBuilder = new StringArray.Builder();
            var fkColumnRefSchemaBuilder = new StringArray.Builder();
            var fkColumnRefTableBuilder = new StringArray.Builder();
            var fkColumnRefColumnBuilder = new StringArray.Builder();
            var fkColumnKeyNameBuilder = new StringArray.Builder();
            var fkColumnKeySeqBuilder = new Int32Array.Builder();

            var pkColumns = new HashSet<string>(descResult.PrimaryKeys);
            var fkColumns = new Dictionary<String, (int,ForeignKeyInfo)>();
            foreach (var fkInfo in descResult.ForeignKeys)
            {
                for (var i = 0; i < fkInfo.LocalColumns.Count; i++)
                {
                    // The order of local key should match the order of ref key, so we need to store the index
                    fkColumns.Add(fkInfo.LocalColumns[i],(i,fkInfo));
                }
            }

            var position = 0;
            foreach (var column in descResult.Columns)
            {
                var baseTypeName = column.Type.Name.ToUpper();
                // Special case for TIMESTAMP_LTZ and INT
                if (baseTypeName == "TIMESTAMP_LTZ" || baseTypeName == "TIMESTAMP_NTZ")
                {
                    baseTypeName = "TIMESTAMP";
                }
                else if (baseTypeName == "INT")
                {
                    baseTypeName = "INTEGER";
                }

                var fullTypeName = column.Type.FullTypeName;
                var colName = column.Name;

                int dataType = (int)column.DataType;

                tableCatBuilder.Append(descResult.CatalogName);
                tableSchemaBuilder.Append(descResult.SchemaName);
                tableNameBuilder.Append(descResult.TableName);

                columnNameBuilder.Append(colName);
                dataTypeBuilder.Append(dataType);
                typeNameBuilder.Append(fullTypeName);
                columnSizeBuilder.Append(column.ColumnSize);
                bufferLengthBuilder.AppendNull();
                decimalDigitsBuilder.Append(column.DecimalDigits);
                numPrecRadixBuilder.Append(column.IsNumber ? 10: null);
                nullableBuilder.Append(column.Nullable ? 1 : 0);
                remarksBuilder.Append(column.Comment ?? "");
                columnDefBuilder.AppendNull();
                sqlDataTypeBuilder.AppendNull();
                sqlDatetimeSubBuilder.AppendNull();
                charOctetLengthBuilder.AppendNull();
                ordinalPositionBuilder.Append(position++);
                isNullableBuilder.Append(column.Nullable ? "YES" : "NO");

                scopeCatalogBuilder.AppendNull();
                scopeSchemaBuilder.AppendNull();
                scopeTableBuilder.AppendNull();
                sourceDataTypeBuilder.AppendNull();

                isAutoIncrementBuilder.Append("NO");
                baseTypeNameBuilder.Append(baseTypeName);

                pkColumnBuilder.Append(pkColumns.Contains(colName) ? colName : null);

                if (fkColumns.ContainsKey(colName))
                {
                    var (idx,fkInfo) = fkColumns[colName];
                    fkColumnRefColumnBuilder.Append(fkInfo.RefColumns[idx]);
                    fkColumnRefCatalogBuilder.Append(fkInfo.RefCatalog);
                    fkColumnRefSchemaBuilder.Append(fkInfo.RefSchema);
                    fkColumnRefTableBuilder.Append(fkInfo.RefTable);
                    fkColumnLocalBuilder.Append(colName);
                    fkColumnKeyNameBuilder.Append(fkInfo.KeyName);
                    fkColumnKeySeqBuilder.Append(1+idx); // FK_KEY_SEQ is 1-based index
                }
                else
                {
                    fkColumnRefColumnBuilder.AppendNull();
                    fkColumnRefCatalogBuilder.AppendNull();
                    fkColumnRefSchemaBuilder.AppendNull();
                    fkColumnRefTableBuilder.AppendNull();
                    fkColumnLocalBuilder.AppendNull();
                    fkColumnKeyNameBuilder.AppendNull();
                    fkColumnKeySeqBuilder.AppendNull();
                }
            }

            var combinedData = new List<IArrowArray>()
            {
                tableCatBuilder.Build(),
                tableSchemaBuilder.Build(),
                tableNameBuilder.Build(),
                columnNameBuilder.Build(),
                dataTypeBuilder.Build(),
                typeNameBuilder.Build(),
                columnSizeBuilder.Build(),
                bufferLengthBuilder.Build(),
                decimalDigitsBuilder.Build(),
                numPrecRadixBuilder.Build(),
                nullableBuilder.Build(),
                remarksBuilder.Build(),
                columnDefBuilder.Build(),
                sqlDataTypeBuilder.Build(),
                sqlDatetimeSubBuilder.Build(),
                charOctetLengthBuilder.Build(),
                ordinalPositionBuilder.Build(),
                isNullableBuilder.Build(),
                scopeCatalogBuilder.Build(),
                scopeSchemaBuilder.Build(),
                scopeTableBuilder.Build(),
                sourceDataTypeBuilder.Build(),
                isAutoIncrementBuilder.Build(),
                baseTypeNameBuilder.Build(),

                // Metadata column for primary key
                pkColumnBuilder.Build(),

                // Metadata columns for foreign key info
                fkColumnRefColumnBuilder.Build(),
                fkColumnRefCatalogBuilder.Build(),
                fkColumnRefSchemaBuilder.Build(),
                fkColumnRefTableBuilder.Build(),
                fkColumnLocalBuilder.Build(),
                fkColumnKeyNameBuilder.Build(),
                fkColumnKeySeqBuilder.Build()
            };

            return new QueryResult(descResult.Columns.Count, new HiveServer2Connection.HiveInfoArrowStream(combinedSchema, combinedData));
        }
    }
}
