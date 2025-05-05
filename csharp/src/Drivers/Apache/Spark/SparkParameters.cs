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

        // access_token is required when authType is oauth and authFlow is 0
        public const string AccessToken = "adbc.spark.access_token";
        public const string AuthType = "adbc.spark.auth_type";
        public const string AuthFlow = "adbc.spark.oauth.auth_flow";
        // client_id and client_secret are required when authType is oauth and authFlow is 1
        public const string OAuthClientId = "adbc.spark.oauth.client_id";
        public const string OAuthClientSecret = "adbc.spark.oauth.client_secret";
        public const string Type = "adbc.spark.type";
        public const string DataTypeConv = "adbc.spark.data_type_conv";
        public const string ConnectTimeoutMilliseconds = "adbc.spark.connect_timeout_ms";
        public const string UserAgentEntry = "adbc.spark.user_agent_entry";
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
