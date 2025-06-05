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

using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.Snowflake
{
    /// <summary>
    /// Configuration settings for working with Snowflake.
    /// </summary>
    internal class SnowflakeTestConfiguration : TestConfiguration
    {
        /// <summary>
        /// The file path location of the driver.
        /// </summary>
        [JsonPropertyName("driverPath")]
        public string DriverPath { get; set; } = string.Empty;

        /// <summary>
        /// The entrypoint of the driver.
        /// </summary>
        [JsonPropertyName("driverEntryPoint")]
        public string DriverEntryPoint { get; set; } = string.Empty;

        /// <summary>
        /// The Snowflake account.
        /// </summary>
        [JsonPropertyName("account")]
        public string Account { get; set; } = string.Empty;

        /// <summary>
        /// The Snowflake host.
        /// </summary>
        [JsonPropertyName("host")]
        public string Host { get; set; } = string.Empty;

        /// <summary>
        /// The Snowflake database.
        /// </summary>
        [JsonPropertyName("database")]
        public string Database { get; set; } = string.Empty;

        /// <summary>
        /// The Snowflake user.
        /// </summary>
        [JsonPropertyName("user")]
        public string User { get; set; } = string.Empty;

        /// <summary>
        /// The Snowflake password (if using).
        /// </summary>
        [JsonPropertyName("password")]
        public string Password { get; set; } = string.Empty;

        /// <summary>
        /// The Snowflake warehouse.
        /// </summary>
        [JsonPropertyName("warehouse")]
        public string Warehouse { get; set; } = string.Empty;

        /// <summary>
        /// The Snowflake setting to use high precision
        /// </summary>
        [JsonPropertyName("useHighPrecision")]
        public bool UseHighPrecision { get; set; } = true;

        /// <summary>
        /// The Snowflake setting indicate the maximum timestamp precision.
        /// </summary>
        [JsonPropertyName("maxTimestampPrecision")]
        public string MaxTimestampPrecision { get; set; } = "nanoseconds";

        /// <summary>
        /// The snowflake Authentication
        /// </summary>
        [JsonPropertyName("authentication")]
        public SnowflakeAuthentication Authentication { get; set; } = new SnowflakeAuthentication();

        /// <summary>
        /// The snowflake Authentication
        /// </summary>
        [JsonPropertyName("roleInfo")]
        public RoleInfo? RoleInfo { get; set; }
    }

    public class SnowflakeAuthentication
    {
        public const string AuthOAuth = "auth_oauth";
        public const string AuthJwt = "auth_jwt";
        public const string AuthSnowflake = "auth_snowflake";

        [JsonPropertyName(AuthOAuth)]
        public OAuthAuthentication? OAuth { get; set; }

        [JsonPropertyName(AuthJwt)]
        public JwtAuthentication? SnowflakeJwt { get; set; }

        [JsonPropertyName(AuthSnowflake)]
        public DefaultAuthentication? Default { get; set; }
    }

    public class OAuthAuthentication
    {
        [JsonPropertyName("token")]
        public string Token { get; set; } = string.Empty;

        [JsonPropertyName("user")]
        public string User { get; set; } = string.Empty;
    }

    public class JwtAuthentication
    {
        [JsonPropertyName("private_key")]
        public string PrivateKey { get; set; } = string.Empty;

        [JsonPropertyName("private_key_file")]
        public string PrivateKeyFile { get; set; } = string.Empty;

        [JsonPropertyName("private_key_pwd")]
        public string PrivateKeyPassPhrase { get; set; } = string.Empty;

        [JsonPropertyName("user")]
        public string User { get; set; } = string.Empty;
    }

    public class DefaultAuthentication
    {
        [JsonPropertyName("user")]
        public string User { get; set; } = string.Empty;

        [JsonPropertyName("password")]
        public string Password { get; set; } = string.Empty;
    }

    public class RoleInfo
    {
        [JsonPropertyName("defaultRole")]
        public string DefaultRole { get; set; } = string.Empty;

        [JsonPropertyName("newRole")]
        public string NewRole { get; set; } = string.Empty;
    }
}
