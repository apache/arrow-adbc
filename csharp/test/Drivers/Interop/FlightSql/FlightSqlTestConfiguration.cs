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

using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Tests.Drivers.Interop.FlightSql
{
    internal class FlightSqlTestConfiguration
    {
        public FlightSqlTestConfiguration()
        {
            this.Environments = new Dictionary<string, FlightSqlTestEnvironment>();
        }

        /// <summary>
        /// The file path location of the driver.
        /// </summary>
        [JsonPropertyName("driverPath")]
        public string? DriverPath { get; set; }

        /// <summary>
        /// The entrypoint of the driver.
        /// </summary>
        [JsonPropertyName("driverEntryPoint")]
        public string? DriverEntryPoint { get; set; }

        /// <summary>
        /// A comma separated list of testable environments.
        /// </summary>
        [JsonPropertyName("testEnvironments")]
        public string? TestableEnvironments { get; set; }

        /// <summary>
        /// The active test environment.
        /// </summary>
        [JsonPropertyName("environments")]
        public Dictionary<string, FlightSqlTestEnvironment> Environments { get; set; }
    }

    internal class FlightSqlTestEnvironment : TestConfiguration
    {
        public FlightSqlTestEnvironment()
        {
            this.RPCCallHeaders = new Dictionary<string, string>();
            this.TableTypes = new List<string>();
            this.SupportsWriteUpdate = false;
            this.SupportsCatalogs = false;
        }

        /// <summary>
        /// The name of the environment.
        /// </summary>
        public string? Name { get; set; }

        /// <summary>
        /// The service URI.
        /// </summary>
        [JsonPropertyName("uri")]
        public string? Uri { get; set; }

        /// <summary>
        /// Additional headers to add to the gRPC call.
        /// </summary>
        [JsonPropertyName("headers")]
        public Dictionary<string, string> RPCCallHeaders { get; set; }

        /// <summary>
        /// Additional headers to add to the gRPC call.
        /// </summary>
        [JsonPropertyName("sqlFile")]

        public string? FlightSqlFile { get; set; }

        /// <summary>
        /// The authorization header.
        /// </summary>
        [JsonPropertyName("authorization")]
        public string? AuthorizationHeader { get; set; }

        [JsonPropertyName("timeoutFetch")]
        public string? TimeoutFetch { get; set; }

        [JsonPropertyName("timeoutQuery")]
        public string? TimeoutQuery { get; set; }

        [JsonPropertyName("timeoutUpdate")]
        public string? TimeoutUpdate { get; set; }

        [JsonPropertyName("sslSkipVerify")]
        public bool SSLSkipVerify { get; set; }

        [JsonPropertyName("authority")]
        public string? Authority { get; set; }

        [JsonPropertyName("username")]
        public string? Username { get; set; }

        [JsonPropertyName("password")]
        public string? Password { get; set; }

        [JsonPropertyName("supportsWriteUpdate")]
        public bool SupportsWriteUpdate { get; set; }

        [JsonPropertyName("supportsCatalogs")]
        public bool SupportsCatalogs { get; set; }

        [JsonPropertyName("tableTypes")]
        public List<string> TableTypes { get; set; }
    }
}