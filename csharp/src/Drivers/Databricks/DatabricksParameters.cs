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

namespace Apache.Arrow.Adbc.Drivers.Databricks
{
    /// <summary>
    /// Parameters used for connecting to Databricks data sources.
    /// </summary>
    public class DatabricksParameters
    {
        // CloudFetch configuration parameters
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
    }

    /// <summary>
    /// Constants used for default parameter values.
    /// </summary>
    public class DatabricksConstants
    {

    }
}
