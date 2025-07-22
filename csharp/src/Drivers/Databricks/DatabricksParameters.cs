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

using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Parameters used for connecting to Databricks data sources.
    /// </summary>
    public class DatabricksParameters : SparkParameters
    {
        // CloudFetch configuration parameters
        /// <summary>
        /// Whether to use CloudFetch for retrieving results.
        /// Default value is true if not specified.
        /// </summary>
        public const string UseCloudFetch = "adbc.databricks.cloudfetch.enabled";

        /// <summary>
        /// Whether the client can decompress LZ4 compressed results.
        /// Default value is true if not specified.
        /// </summary>
        public const string CanDecompressLz4 = "adbc.databricks.cloudfetch.lz4.enabled";

        /// <summary>
        /// Maximum bytes per file for CloudFetch.
        /// Default value is 20MB if not specified.
        /// </summary>
        public const string MaxBytesPerFile = "adbc.databricks.cloudfetch.max_bytes_per_file";

        /// <summary>
        /// Maximum number of retry attempts for CloudFetch downloads.
        /// Default value is 3 if not specified.
        /// </summary>
        public const string CloudFetchMaxRetries = "adbc.databricks.cloudfetch.max_retries";

        /// <summary>
        /// Delay in milliseconds between CloudFetch retry attempts.
        /// Default value is 500ms if not specified.
        /// </summary>
        public const string CloudFetchRetryDelayMs = "adbc.databricks.cloudfetch.retry_delay_ms";

        /// <summary>
        /// Timeout in minutes for CloudFetch HTTP operations.
        /// Default value is 5 minutes if not specified.
        /// </summary>
        public const string CloudFetchTimeoutMinutes = "adbc.databricks.cloudfetch.timeout_minutes";

        /// <summary>
        /// Buffer time in seconds before URL expiration to trigger refresh.
        /// Default value is 60 seconds if not specified.
        /// </summary>
        public const string CloudFetchUrlExpirationBufferSeconds = "adbc.databricks.cloudfetch.url_expiration_buffer_seconds";

        /// <summary>
        /// Maximum number of URL refresh attempts for CloudFetch downloads.
        /// Default value is 3 if not specified.
        /// </summary>
        public const string CloudFetchMaxUrlRefreshAttempts = "adbc.databricks.cloudfetch.max_url_refresh_attempts";

        /// <summary>
        /// Whether to enable the use of direct results when executing queries.
        /// Default value is true if not specified.
        /// </summary>
        public const string EnableDirectResults = "adbc.databricks.enable_direct_results";

        /// <summary>
        /// Whether to apply service side properties (SSP) with queries. If false, SSP will be applied
        /// by setting the Thrift configuration when the session is opened.
        /// Default value is false if not specified.
        /// </summary>
        public const string ApplySSPWithQueries = "adbc.databricks.apply_ssp_with_queries";


        /// <summary>
        /// Prefix for server-side properties. Properties with this prefix will be passed to the server
        /// by executing a "set key=value" query when opening a session.
        /// For example, a property with key "adbc.databricks.ssp_use_cached_result"
        /// and value "true" will result in executing "set use_cached_result=true" on the server.
        /// </summary>
        public const string ServerSidePropertyPrefix = "adbc.databricks.ssp_";

        /// Controls whether to retry requests that receive a 503 response with a Retry-After header.
        /// Default value is true (enabled). Set to false to disable retry behavior.
        /// </summary>
        public const string TemporarilyUnavailableRetry = "adbc.spark.temporarily_unavailable_retry";

        /// <summary>
        /// Maximum total time in seconds to retry 503 responses before failing.
        /// Default value is 900 seconds (15 minutes). Set to 0 to retry indefinitely.
        /// </summary>
        public const string TemporarilyUnavailableRetryTimeout = "adbc.spark.temporarily_unavailable_retry_timeout";

        /// <summary>
        /// Maximum number of parallel downloads for CloudFetch operations.
        /// Default value is 3 if not specified.
        /// </summary>
        public const string CloudFetchParallelDownloads = "adbc.databricks.cloudfetch.parallel_downloads";

        /// <summary>
        /// Number of files to prefetch in CloudFetch operations.
        /// Default value is 2 if not specified.
        /// </summary>
        public const string CloudFetchPrefetchCount = "adbc.databricks.cloudfetch.prefetch_count";

        /// <summary>
        /// Maximum memory buffer size in MB for CloudFetch prefetched files.
        /// Default value is 200MB if not specified.
        /// </summary>
        public const string CloudFetchMemoryBufferSize = "adbc.databricks.cloudfetch.memory_buffer_size_mb";

        /// <summary>
        /// Whether CloudFetch prefetch functionality is enabled.
        /// Default value is true if not specified.
        /// </summary>
        public const string CloudFetchPrefetchEnabled = "adbc.databricks.cloudfetch.prefetch_enabled";

        /// <summary>
        /// The OAuth grant type to use for authentication.
        /// Supported values:
        /// - "access_token": Use a pre-generated Databricks personal access token (default)
        /// - "client_credentials": Use OAuth client credentials flow for m2m authentication
        /// When using "client_credentials", the driver will automatically handle token acquisition,
        /// renewal, and authentication with the Databricks service.
        /// </summary>
        public const string OAuthGrantType = "adbc.databricks.oauth.grant_type";

        /// <summary>
        /// The OAuth client ID for client credentials flow.
        /// Required when grant_type is "client_credentials".
        /// This is the client ID you obtained when registering your application with Databricks.
        /// </summary>
        public const string OAuthClientId = "adbc.databricks.oauth.client_id";

        /// <summary>
        /// The OAuth client secret for client credentials flow.
        /// Required when grant_type is "client_credentials".
        /// This is the client secret you obtained when registering your application with Databricks.
        /// </summary>
        public const string OAuthClientSecret = "adbc.databricks.oauth.client_secret";

        /// <summary>
        /// The OAuth scope for client credentials flow.
        /// Optional when grant_type is "client_credentials".
        /// Default value is "sql" if not specified.
        /// </summary>
        public const string OAuthScope = "adbc.databricks.oauth.scope";

        /// <summary>
        /// Whether to use multiple catalogs.
        /// Default value is true if not specified.
        /// </summary>
        public const string EnableMultipleCatalogSupport = "adbc.databricks.enable_multiple_catalog_support";

        /// <summary>
        /// Whether to enable primary key foreign key metadata call.
        /// Default value is true if not specified.
        /// </summary>
        public const string EnablePKFK = "adbc.databricks.enable_pk_fk";

        /// <summary>
        /// Whether to use query DESC TABLE EXTENDED to get extended column metadata when the current DBR supports it
        /// Default value is true if not specified.
        /// </summary>
        public const string UseDescTableExtended = "adbc.databricks.use_desc_table_extended";

        /// <summary>
        /// Whether to enable RunAsync flag in Thrift operation
        /// Default value is false if not specified.
        /// </summary>
        public const string EnableRunAsyncInThriftOp = "adbc.databricks.enable_run_async_thrift";

        /// <summary>
        /// Whether to propagate trace parent headers in HTTP requests.
        /// Default value is true if not specified.
        /// When enabled, the driver will add W3C Trace Context headers to all HTTP requests.
        /// </summary>
        public const string TracePropagationEnabled = "adbc.databricks.trace_propagation.enabled";

        /// <summary>
        /// The name of the HTTP header to use for trace parent propagation.
        /// Default value is "traceparent" (W3C standard) if not specified.
        /// This allows customization for systems that use different header names.
        /// </summary>
        public const string TraceParentHeaderName = "adbc.databricks.trace_propagation.header_name";

        /// <summary>
        /// Whether to include trace state header in HTTP requests.
        /// Default value is false if not specified.
        /// When enabled, the driver will also propagate the tracestate header if available.
        /// </summary>
        public const string TraceStateEnabled = "adbc.databricks.trace_propagation.state_enabled";

        /// <summary>
        /// The minutes before token expiration when we should start renewing the token.
        /// Default value is 0 (disabled) if not specified.
        /// </summary>
        public const string TokenRenewLimit = "adbc.databricks.token_renew_limit";
    }

    /// <summary>
    /// Constants used for default parameter values.
    /// </summary>
    public class DatabricksConstants
    {
        /// <summary>
        /// Default heartbeat interval in seconds for long-running operations
        /// </summary>
        public const int DefaultOperationStatusPollingIntervalSeconds = 60;

        /// <summary>
        /// OAuth grant type constants
        /// </summary>
        public static class OAuthGrantTypes
        {
            /// <summary>
            /// Use a pre-generated Databricks personal access token for authentication.
            /// When using this grant type, you must provide the token via the
            /// adbc.spark.oauth.access_token parameter.
            /// </summary>
            public const string AccessToken = "access_token";

            /// <summary>
            /// Use OAuth client credentials flow for m2m authentication.
            /// When using this grant type, you must provide:
            /// - adbc.databricks.oauth.client_id: The OAuth client ID
            /// - adbc.databricks.oauth.client_secret: The OAuth client secret
            /// The driver will automatically handle token acquisition, renewal, and
            /// authentication with the Databricks service.
            /// </summary>
            public const string ClientCredentials = "client_credentials";
        }
    }
}
