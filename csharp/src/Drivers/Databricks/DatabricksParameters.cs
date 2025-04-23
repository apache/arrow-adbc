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
        /// For example, a property with key "adbc.databricks.SSP_use_cached_result"
        /// and value "true" will result in executing "set use_cached_result=true" on the server.
        /// </summary>
        public const string ServerSidePropertyPrefix = "adbc.databricks.SSP_";
        /// Controls whether to retry requests that receive a 503 response with a Retry-After header.
        /// Default value is true (enabled). Set to false to disable retry behavior.
        /// </summary>
        public const string TemporarilyUnavailableRetry = "adbc.spark.temporarily_unavailable_retry";

        /// <summary>
        /// Maximum total time in seconds to retry 503 responses before failing.
        /// Default value is 900 seconds (15 minutes). Set to 0 to retry indefinitely.
        /// </summary>
        public const string TemporarilyUnavailableRetryTimeout = "adbc.spark.temporarily_unavailable_retry_timeout";
    }

    /// <summary>
    /// Constants used for default parameter values.
    /// </summary>
    public class DatabricksConstants
    {

    }
}
