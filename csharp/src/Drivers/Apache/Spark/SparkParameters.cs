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

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    /// <summary>
    /// Parameters used for connecting to Spark data sources.
    /// </summary>
    public class SparkParameters
    {
        public const string HostName = "adbc.spark.host";
        public const string Port = "adbc.spark.port";
        public const string Path = "adbc.spark.path";
        public const string Token = "adbc.spark.token";

        // access_token is required when authType is oauth
        public const string AccessToken = "adbc.spark.access_token";
        public const string AuthType = "adbc.spark.auth_type";
        public const string Type = "adbc.spark.type";
        public const string DataTypeConv = "adbc.spark.data_type_conv";
        public const string ConnectTimeoutMilliseconds = "adbc.spark.connect_timeout_ms";
        public const string UserAgentEntry = "adbc.spark.user_agent_entry";
        
        // Proxy configuration parameters
        /// <summary>
        /// Whether to use a proxy for HTTP connections.
        /// Default value is 0 (disabled) if not specified.
        /// </summary>
        public const string UseProxy = "UseProxy";

        /// <summary>
        /// Hostname or IP address of the proxy server.
        /// Required when UseProxy is set to 1.
        /// </summary>
        public const string ProxyHost = "ProxyHost";

        /// <summary>
        /// Port number of the proxy server.
        /// Default value is 8080 if not specified.
        /// </summary>
        public const string ProxyPort = "ProxyPort";

        /// <summary>
        /// Comma-separated list of hosts or domains that should bypass the proxy.
        /// For example: "localhost,127.0.0.1,.internal.domain.com"
        /// </summary>
        public const string ProxyIgnoreList = "ProxyIgnoreList";

        /// <summary>
        /// Whether to enable proxy authentication.
        /// Default value is 0 (disabled) if not specified.
        /// </summary>
        public const string ProxyAuth = "ProxyAuth";

        /// <summary>
        /// Username for proxy authentication.
        /// Required when ProxyAuth is set to 1.
        /// </summary>
        public const string ProxyUID = "ProxyUID";

        /// <summary>
        /// Password for proxy authentication.
        /// </summary>
        public const string ProxyPWD = "ProxyPWD";
    }

    public static class SparkAuthTypeConstants
    {
        public const string None = "none";
        public const string UsernameOnly = "username_only";
        public const string Basic = "basic";
        public const string Token = "token";
        public const string OAuth = "oauth";
    }

    public static class SparkServerTypeConstants
    {
        public const string Http = "http";
        public const string Standard = "standard";
    }
}
