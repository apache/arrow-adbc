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
using Apache.Arrow.Adbc.Drivers.Databricks.Result;
using Apache.Arrow.Adbc.Tracing;
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
        // Databricks CloudFetch supports much larger batch sizes than standard Arrow batches (1024MB vs 10MB limit).
        // Using 2M rows significantly reduces round trips for medium/large result sets compared to the base 50K default,
        // improving query performance by reducing the number of FetchResults calls needed.
        private const long DatabricksBatchSizeDefault = 2000000;
        private bool useCloudFetch;
        private bool canDecompressLz4;
        private long maxBytesPerFile;
        private long maxBytesPerFetchRequest;
        private bool enableMultipleCatalogSupport;
        private bool enablePKFK;
        private bool runAsyncInThrift;

        public override long BatchSize { get; protected set; } = DatabricksBatchSizeDefault;

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
            maxBytesPerFetchRequest = connection.MaxBytesPerFetchRequest;
            enableMultipleCatalogSupport = connection.EnableMultipleCatalogSupport;
            enablePKFK = connection.EnablePKFK;

            runAsyncInThrift = connection.RunAsyncInThrift;

            // Override the Apache base default (500ms) with Databricks-specific poll interval (100ms)
            if (!connection.Properties.ContainsKey(ApacheParameters.PollTimeMilliseconds))
            {
                SetOption(ApacheParameters.PollTimeMilliseconds, DatabricksConstants.DefaultAsyncExecPollIntervalMs.ToString());
            }
        }

        /// <summary>
        /// Gets the schema from metadata response. Handles both Arrow schema (Protocol V5+) and traditional Thrift schema.
        /// </summary>
        /// <param name="metadata">The metadata response containing schema information</param>
        /// <returns>The Arrow schema</returns>
        protected override Schema GetSchemaFromMetadata(TGetResultSetMetadataResp metadata)
        {
            // Log schema parsing decision
            Activity.Current?.SetTag("statement.schema.has_arrow_schema", metadata.__isset.arrowSchema);

            // For Protocol V5+, prefer Arrow schema if available
            if (metadata.__isset.arrowSchema)
            {
                Schema? arrowSchema = ((DatabricksSchemaParser)Connection.SchemaParser).ParseArrowSchema(metadata.ArrowSchema);
                if (arrowSchema != null)
                {
                    Activity.Current?.SetTag("statement.schema.source", "arrow");
                    Activity.Current?.SetTag("statement.schema.column_count", arrowSchema.FieldsList.Count);
                    Activity.Current?.AddEvent("statement.schema.using_arrow_schema");
                    return arrowSchema;
                }
            }

            // Fallback to traditional Thrift schema
            Activity.Current?.SetTag("statement.schema.source", "thrift");
            Activity.Current?.AddEvent("statement.schema.fallback_to_thrift");
            var thriftSchema = Connection.SchemaParser.GetArrowSchema(metadata.Schema, Connection.DataTypeConversion);
            Activity.Current?.SetTag("statement.schema.column_count", thriftSchema.FieldsList.Count);
            return thriftSchema;
        }

        protected override void SetStatementProperties(TExecuteStatementReq statement)
        {
            Activity.Current?.AddEvent("statement.set_properties.start");

            base.SetStatementProperties(statement);

            // Set Databricks-specific statement properties
            // TODO: Ensure this is set dynamically depending on server capabilities.
            statement.EnforceResultPersistenceMode = false;
            statement.CanReadArrowResult = true;

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

            Connection.TrySetGetDirectResults(statement);

            // Log Databricks-specific properties
            Activity.Current?.SetTag("statement.property.enforce_result_persistence_mode", statement.EnforceResultPersistenceMode);
            Activity.Current?.SetTag("statement.property.can_read_arrow_result", statement.CanReadArrowResult);
            Activity.Current?.SetTag("statement.property.timestamp_as_arrow", statement.UseArrowNativeTypes.TimestampAsArrow);
            Activity.Current?.SetTag("statement.property.decimal_as_arrow", statement.UseArrowNativeTypes.DecimalAsArrow);
            Activity.Current?.SetTag("statement.property.complex_types_as_arrow", statement.UseArrowNativeTypes.ComplexTypesAsArrow);
            Activity.Current?.SetTag("statement.property.interval_types_as_arrow", statement.UseArrowNativeTypes.IntervalTypesAsArrow);

            // Log CloudFetch configuration
            Activity.Current?.SetTag("statement.cloudfetch.enabled", useCloudFetch);
            Activity.Current?.SetTag("statement.cloudfetch.can_decompress_lz4", canDecompressLz4);
            Activity.Current?.SetTag("statement.cloudfetch.max_bytes_per_file", maxBytesPerFile);
            Activity.Current?.SetTag("statement.cloudfetch.max_bytes_per_file_mb", maxBytesPerFile / 1024.0 / 1024.0);
            Activity.Current?.SetTag("statement.property.run_async", runAsyncInThrift);

            Activity.Current?.AddEvent("statement.set_properties.complete");
        }

        // Cast the Client to IAsync for CloudFetch compatibility
        TCLIService.IAsync IHiveServer2Statement.Client => Connection.Client;

        // Expose QueryTimeoutSeconds for IHiveServer2Statement
        int IHiveServer2Statement.QueryTimeoutSeconds => base.QueryTimeoutSeconds;

        // Expose BatchSize through the interface
        long IHiveServer2Statement.BatchSize => BatchSize;

        // Expose Connection through the interface
        HiveServer2Connection IHiveServer2Statement.Connection => Connection;

        public override void SetOption(string key, string value)
        {
            Activity.Current?.AddEvent("statement.set_option", [
                new("option_key", key),
                new("option_value", value)
            ]);

            switch (key)
            {
                case DatabricksParameters.UseCloudFetch:
                    if (bool.TryParse(value, out bool useCloudFetchValue))
                    {
                        this.useCloudFetch = useCloudFetchValue;
                        Activity.Current?.SetTag("statement.cloudfetch.enabled", this.useCloudFetch);
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
                        Activity.Current?.SetTag("statement.cloudfetch.can_decompress_lz4", this.canDecompressLz4);
                    }
                    else
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Expected a boolean value.");
                    }
                    break;
                case DatabricksParameters.MaxBytesPerFile:
                    try
                    {
                        long maxBytesPerFileValue = DatabricksConnection.ParseBytesWithUnits(value);
                        this.maxBytesPerFile = maxBytesPerFileValue;
                        Activity.Current?.SetTag("statement.cloudfetch.max_bytes_per_file", this.maxBytesPerFile);
                        Activity.Current?.SetTag("statement.cloudfetch.max_bytes_per_file_mb", this.maxBytesPerFile / 1024.0 / 1024.0);
                    }
                    catch (FormatException)
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Valid formats: number with optional unit suffix (B, KB, MB, GB). Examples: '20MB', '1024KB', '1073741824'.");
                    }
                    break;
                case DatabricksParameters.MaxBytesPerFetchRequest:
                    try
                    {
                        long maxBytesPerFetchRequestValue = DatabricksConnection.ParseBytesWithUnits(value);
                        this.maxBytesPerFetchRequest = maxBytesPerFetchRequestValue;
                        Activity.Current?.SetTag("statement.cloudfetch.max_bytes_per_fetch_request", this.maxBytesPerFetchRequest);
                        Activity.Current?.SetTag("statement.cloudfetch.max_bytes_per_fetch_request_mb", this.maxBytesPerFetchRequest / 1024.0 / 1024.0);
                    }
                    catch (FormatException)
                    {
                        throw new ArgumentException($"Invalid value for {key}: {value}. Valid formats: number with optional unit suffix (B, KB, MB, GB). Examples: '400MB', '1024KB', '1073741824'.");
                    }
                    break;
                case ApacheParameters.BatchSize:
                    if (long.TryParse(value, out long batchSize) && batchSize > 0)
                    {
                        this.BatchSize = batchSize;
                        Activity.Current?.SetTag("statement.batch_size", this.BatchSize);
                    }
                    else
                    {
                        throw new ArgumentOutOfRangeException(key, value, $"The value '{value}' for option '{key}' is invalid. Must be a numeric value greater than zero.");
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
        /// Gets the maximum bytes per fetch request.
        /// </summary>
        public long MaxBytesPerFetchRequest => maxBytesPerFetchRequest;

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
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_catalogs.start");
                activity?.SetTag("statement.feature.enable_multiple_catalog_support", enableMultipleCatalogSupport);

                // If EnableMultipleCatalogSupport is false, return a single catalog "SPARK" without making an RPC call
                if (!enableMultipleCatalogSupport)
                {
                    activity?.AddEvent("statement.get_catalogs.returning_spark_catalog", [
                        new("reason", "Multiple catalog support disabled")
                    ]);

                    // Create a schema with a single column TABLE_CAT
                    var field = new Field("TABLE_CAT", StringType.Default, true);
                    var schema = new Schema(new[] { field }, null);

                    // Create a single row with value "SPARK"
                    var builder = new StringArray.Builder();
                    builder.Append("SPARK");
                    var array = builder.Build();

                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 1);
                    activity?.AddEvent("statement.get_catalogs.complete");

                    // Return the result without making an RPC call
                    return new QueryResult(1, new HiveServer2Connection.HiveInfoArrowStream(schema, new[] { array }));
                }

                // If EnableMultipleCatalogSupport is true, delegate to base class implementation
                activity?.AddEvent("statement.get_catalogs.calling_base_implementation");
                QueryResult result = await base.GetCatalogsAsync(cancellationToken);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, result.RowCount);
                activity?.AddEvent("statement.get_catalogs.complete");
                return result;
            }, activityName: "GetCatalogs");
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
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_schemas.start");
                activity?.SetTag("statement.catalog_name", CatalogName ?? "(none)");
                activity?.SetTag("statement.feature.enable_multiple_catalog_support", enableMultipleCatalogSupport);

                // Handle SPARK catalog case
                HandleSparkCatalog();
                activity?.SetTag("statement.catalog_name_after_spark_handling", CatalogName ?? "(none)");

                // If EnableMultipleCatalogSupport is false and catalog is not null or SPARK, return empty result without RPC call
                if (!enableMultipleCatalogSupport && CatalogName != null)
                {
                    activity?.AddEvent("statement.get_schemas.returning_empty_result", [
                        new("reason", "Multiple catalog support disabled and catalog is not null")
                    ]);

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

                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                    activity?.AddEvent("statement.get_schemas.complete");

                    // Return empty result
                    return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, new[] { catalogArray, schemaArray }));
                }

                // Call the base implementation with the potentially modified catalog name
                activity?.AddEvent("statement.get_schemas.calling_base_implementation");
                QueryResult result = await base.GetSchemasAsync(cancellationToken);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, result.RowCount);
                activity?.AddEvent("statement.get_schemas.complete");
                return result;
            }, activityName: "GetSchemas");
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
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_tables.start");
                activity?.SetTag("statement.catalog_name", CatalogName ?? "(none)");
                activity?.SetTag("statement.schema_name", SchemaName ?? "(none)");
                activity?.SetTag("statement.table_name", TableName ?? "(none)");
                activity?.SetTag("statement.table_types", TableTypes ?? "(none)");
                activity?.SetTag("statement.feature.enable_multiple_catalog_support", enableMultipleCatalogSupport);

                // Handle SPARK catalog case
                HandleSparkCatalog();
                activity?.SetTag("statement.catalog_name_after_spark_handling", CatalogName ?? "(none)");

                // If EnableMultipleCatalogSupport is false and catalog is not null or SPARK, return empty result without RPC call
                if (!enableMultipleCatalogSupport && CatalogName != null)
                {
                    activity?.AddEvent("statement.get_tables.returning_empty_result", [
                        new("reason", "Multiple catalog support disabled and catalog is not null")
                    ]);

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

                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                    activity?.AddEvent("statement.get_tables.complete");

                    // Return empty result
                    return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, arrays));
                }

                // Call the base implementation with the potentially modified catalog name
                activity?.AddEvent("statement.get_tables.calling_base_implementation");
                QueryResult result = await base.GetTablesAsync(cancellationToken);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, result.RowCount);
                activity?.AddEvent("statement.get_tables.complete");
                return result;
            }, activityName: "GetTables");
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
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_columns.start");
                activity?.SetTag("statement.catalog_name", CatalogName ?? "(none)");
                activity?.SetTag("statement.schema_name", SchemaName ?? "(none)");
                activity?.SetTag("statement.table_name", TableName ?? "(none)");
                activity?.SetTag("statement.column_name", ColumnName ?? "(none)");
                activity?.SetTag("statement.feature.enable_multiple_catalog_support", enableMultipleCatalogSupport);

                // Handle SPARK catalog case
                HandleSparkCatalog();
                activity?.SetTag("statement.catalog_name_after_spark_handling", CatalogName ?? "(none)");

                // If EnableMultipleCatalogSupport is false and catalog is not null, return empty result without RPC call
                if (!enableMultipleCatalogSupport && CatalogName != null)
                {
                    activity?.AddEvent("statement.get_columns.returning_empty_result", [
                        new("reason", "Multiple catalog support disabled and catalog is not null")
                    ]);

                    // Correct schema for GetColumns
                    var schema = CreateColumnMetadataSchema();

                    // Create empty arrays for all columns
                    var arrays = CreateColumnMetadataEmptyArray();

                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                    activity?.AddEvent("statement.get_columns.complete");

                    // Return empty result
                    return new QueryResult(0, new HiveServer2Connection.HiveInfoArrowStream(schema, arrays));
                }

                // Call the base implementation with the potentially modified catalog name
                activity?.AddEvent("statement.get_columns.calling_base_implementation");
                QueryResult result = await base.GetColumnsAsync(cancellationToken);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, result.RowCount);
                activity?.AddEvent("statement.get_columns.complete");
                return result;
            }, activityName: "GetColumns");
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
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_primary_keys.start");
                activity?.SetTag("statement.catalog_name", CatalogName ?? "(none)");
                activity?.SetTag("statement.schema_name", SchemaName ?? "(none)");
                activity?.SetTag("statement.table_name", TableName ?? "(none)");
                activity?.SetTag("statement.feature.pk_fk_enabled", enablePKFK);

                if (ShouldReturnEmptyPkFkResult())
                {
                    activity?.AddEvent("statement.get_primary_keys.returning_empty_result", [
                        new("reason", !enablePKFK ? "PK/FK feature disabled" : "Invalid catalog for PK/FK"),
                        new("catalog_name", CatalogName ?? "(none)")
                    ]);
                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                    activity?.AddEvent("statement.get_primary_keys.complete");
                    return EmptyPrimaryKeysResult();
                }

                activity?.AddEvent("statement.get_primary_keys.calling_base_implementation");
                QueryResult result = await base.GetPrimaryKeysAsync(cancellationToken);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, result.RowCount);
                activity?.AddEvent("statement.get_primary_keys.complete");
                return result;
            }, activityName: "GetPrimaryKeys");
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
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_cross_reference.start");
                activity?.SetTag("statement.catalog_name", CatalogName ?? "(none)");
                activity?.SetTag("statement.schema_name", SchemaName ?? "(none)");
                activity?.SetTag("statement.table_name", TableName ?? "(none)");
                activity?.SetTag("statement.foreign_catalog_name", ForeignCatalogName ?? "(none)");
                activity?.SetTag("statement.foreign_schema_name", ForeignSchemaName ?? "(none)");
                activity?.SetTag("statement.foreign_table_name", ForeignTableName ?? "(none)");
                activity?.SetTag("statement.feature.pk_fk_enabled", enablePKFK);

                if (ShouldReturnEmptyPkFkResult())
                {
                    activity?.AddEvent("statement.get_cross_reference.returning_empty_result", [
                        new("reason", !enablePKFK ? "PK/FK feature disabled" : "Invalid catalog for PK/FK")
                    ]);
                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                    activity?.AddEvent("statement.get_cross_reference.complete");
                    return EmptyCrossReferenceResult();
                }

                activity?.AddEvent("statement.get_cross_reference.calling_base_implementation");
                QueryResult result = await base.GetCrossReferenceAsync(cancellationToken);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, result.RowCount);
                activity?.AddEvent("statement.get_cross_reference.complete");
                return result;
            }, activityName: "GetCrossReference");
        }

        protected override async Task<QueryResult> GetCrossReferenceAsForeignTableAsync(CancellationToken cancellationToken = default)
        {
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_cross_reference_as_foreign_table.start");
                activity?.SetTag("statement.catalog_name", CatalogName ?? "(none)");
                activity?.SetTag("statement.foreign_catalog_name", ForeignCatalogName ?? "(none)");
                activity?.SetTag("statement.feature.pk_fk_enabled", enablePKFK);

                if (ShouldReturnEmptyPkFkResult())
                {
                    activity?.AddEvent("statement.get_cross_reference_as_foreign_table.returning_empty_result", [
                        new("reason", !enablePKFK ? "PK/FK feature disabled" : "Invalid catalog for PK/FK")
                    ]);
                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                    activity?.AddEvent("statement.get_cross_reference_as_foreign_table.complete");
                    return EmptyCrossReferenceResult();
                }

                activity?.AddEvent("statement.get_cross_reference_as_foreign_table.calling_base_implementation");
                QueryResult result = await base.GetCrossReferenceAsForeignTableAsync(cancellationToken);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, result.RowCount);
                activity?.AddEvent("statement.get_cross_reference_as_foreign_table.complete");
                return result;
            }, activityName: "GetCrossReferenceAsForeignTable");
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
            return await this.TraceActivityAsync(async activity =>
            {
                activity?.AddEvent("statement.get_columns_extended.start");
                string? fullTableName = BuildTableName();
                var canUseDescTableExtended = ((DatabricksConnection)Connection).CanUseDescTableExtended;

                activity?.SetTag("statement.catalog_name", CatalogName ?? "(none)");
                activity?.SetTag("statement.schema_name", SchemaName ?? "(none)");
                activity?.SetTag("statement.table_name", TableName ?? "(none)");
                activity?.SetTag("statement.desc_table_extended.full_table_name", fullTableName ?? "(none)");
                activity?.SetTag("statement.desc_table_extended.can_use", canUseDescTableExtended);

                if (!canUseDescTableExtended || string.IsNullOrEmpty(fullTableName))
                {
                    activity?.AddEvent("statement.get_columns_extended.fallback_to_base", [
                        new("reason", !canUseDescTableExtended ? "DESC TABLE EXTENDED not available" : "Full table name is empty")
                    ]);
                    // When fullTableName is empty, we cannot use metadata SQL query to get the info,
                    // so fallback to base class implementation
                    QueryResult baseResult = await base.GetColumnsExtendedAsync(cancellationToken);
                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, baseResult.RowCount);
                    activity?.AddEvent("statement.get_columns_extended.complete");
                    return baseResult;
                }

                string query = $"DESC TABLE EXTENDED {fullTableName} AS JSON";
                activity?.AddEvent("statement.desc_table_extended.executing_query", [
                    new("query_summary", query.Length > 100 ? query.Substring(0, 100) + "..." : query)
                ]);

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
                    activity?.AddEvent("statement.desc_table_extended.query_failed_fallback_to_base", [
                        new("sql_state", ex.SqlState),
                        new("error_message", ex.Message)
                    ]);
                    Debug.WriteLine($"[WARN] Failed to run {query} (reason={ex.Message}). Fallback to base::GetColumnsExtendedAsync.");
                    QueryResult baseResult = await base.GetColumnsExtendedAsync(cancellationToken);
                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, baseResult.RowCount);
                    activity?.AddEvent("statement.get_columns_extended.complete");
                    return baseResult;
                }

                var columnMetadataSchema = CreateColumnMetadataSchema();

                if (descResult.Stream == null)
                {
                    activity?.AddEvent("statement.desc_table_extended.empty_stream");
                    activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                    activity?.AddEvent("statement.get_columns_extended.complete");
                    return CreateEmptyExtendedColumnsResult(columnMetadataSchema);
                }

                // Read the json result
                activity?.AddEvent("statement.desc_table_extended.parsing_result");
                var resultJson = "";
                using (var stream = descResult.Stream)
                {
                    var batch = await stream.ReadNextRecordBatchAsync(cancellationToken);
                    if (batch == null || batch.Length == 0)
                    {
                        activity?.AddEvent("statement.desc_table_extended.empty_batch");
                        activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, 0);
                        activity?.AddEvent("statement.get_columns_extended.complete");
                        return CreateEmptyExtendedColumnsResult(columnMetadataSchema);
                    }

                    resultJson = ((StringArray)batch.Column(0)).GetString(0);
                    activity?.SetTag("statement.desc_table_extended.result_json_length", resultJson?.Length ?? 0);
                }

                // Parse the JSON result
                if (string.IsNullOrEmpty(resultJson))
                {
                    throw new FormatException($"Invalid json result of {query}: result is null or empty");
                }
                var result = JsonSerializer.Deserialize<DescTableExtendedResult>(resultJson!);
                if (result == null)
                {
                    throw new FormatException($"Invalid json result of {query}.Result={resultJson}");
                }

                activity?.SetTag("statement.desc_table_extended.column_count", result.Columns?.Count ?? 0);
                activity?.SetTag("statement.desc_table_extended.pk_count", result.PrimaryKeys?.Count ?? 0);
                activity?.SetTag("statement.desc_table_extended.fk_count", result.ForeignKeys?.Count ?? 0);

                QueryResult finalResult = CreateExtendedColumnsResult(columnMetadataSchema, result);
                activity?.SetTag(SemanticConventions.Db.Response.ReturnedRows, finalResult.RowCount);
                activity?.AddEvent("statement.get_columns_extended.complete");
                return finalResult;
            }, activityName: "GetColumnsExtended");
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
