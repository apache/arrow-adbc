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
    }
}
