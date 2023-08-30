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

namespace Apache.Arrow.Adbc.Tests.Drivers.Snowflake
{
    /// <summary>
    /// Configuration settings for working with Snowflake.
    /// </summary>
    internal class SnowflakeTestConfiguration : TestConfiguration
    {
        [JsonPropertyName("driverPath")]
        public string DriverPath { get; set; }

        [JsonPropertyName("driverEntryPoint")]
        public string DriverEntryPoint { get; set; }

        [JsonPropertyName("account")]
        public string Account { get; set; }

        [JsonPropertyName("user")]
        public string User { get; set; }

        [JsonPropertyName("password")]
        public string Password { get; set; }

        [JsonPropertyName("warehouse")]
        public string Warehouse { get; set; }

        [JsonPropertyName("authenticationType")]
        public string AuthenticationType { get; set; }

        [JsonPropertyName("authenticationTokenPath"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string AuthenticationTokenPath { get; set; }
    }
}
