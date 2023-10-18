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
        public string DriverPath { get; set; }

        /// <summary>
        /// The entrypoint of the driver.
        /// </summary>
        [JsonPropertyName("driverEntryPoint")]
        public string DriverEntryPoint { get; set; }

        /// <summary>
        /// The snowflake Authentication
        /// </summary>
        [JsonPropertyName("authentication")]
        public SnowflakeAuthentication Authentication { get; set; }

        /// <summary>
        /// The Snowflake account.
        /// </summary>
        [JsonPropertyName("account")]
        public string Account { get; set; }

        /// <summary>
        /// The Snowflake user.
        /// </summary>
        [JsonPropertyName("user")]
        public string User { get; set; }

        /// <summary>
        /// The Snowflake password (if using).
        /// </summary>
        [JsonPropertyName("password")]
        public string Password { get; set; }

        /// <summary>
        /// The Snowflake warehouse.
        /// </summary>
        [JsonPropertyName("warehouse")]
        public string Warehouse { get; set; }

        /// <summary>
        /// The Snowflake authentication type.
        /// </summary>
        [JsonPropertyName("authenticationType")]
        public string AuthenticationType { get; set; }

        /// <summary>
        /// The file location of the authentication token (if using).
        /// </summary>
        [JsonPropertyName("authenticationTokenPath"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string AuthenticationTokenPath { get; set; }

        /// <summary>
        /// The Authentication token.
        /// </summary>
        [JsonPropertyName("authenticationToken")]
        public string AuthenticationToken { get; set; }

    }


    internal class SnowflakeAuthentication
    {
        public const string AuthOAuth = "auth_oauth";
        public const string AuthJwt = "auth_jwt";
        public const string AuthDefault = "default";

        [JsonPropertyName(AuthOAuth)]
        public OAuthAuthentication OAuth { get; set; }

        [JsonPropertyName(AuthJwt)]
        public JwtAuthentication SnowflakeJwt { get; set; }

        [JsonPropertyName(AuthDefault)]
        public DefaultAuthentication Default { get; set; }
    }

    internal class OAuthAuthentication
    {
        [JsonPropertyName("token")]
        public string Token { get; set; }

        [JsonPropertyName("user")]
        public string User { get; set; }
    }

    internal class JwtAuthentication
    {
        [JsonPropertyName("private_key")]
        public string PrivateKey { get; set; }

        [JsonPropertyName("private_key_file")]
        public string PrivateKeyFile { get; set; }

        [JsonPropertyName("private_key_pwd")]
        public string PrivateKeyPassword { get; set; }

        [JsonPropertyName("user")]
        public string User { get; set; }
    }

    internal class DefaultAuthentication
    {
        [JsonPropertyName("user")]
        public string User { get; set; }

        [JsonPropertyName("password")]
        public string Password { get; set; }
    }

}
