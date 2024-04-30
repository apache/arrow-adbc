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

namespace Apache.Arrow.Adbc.Tests.Drivers.BigQuery
{
    /// <summary>
    /// Configuration settings for working with BigQuery.
    /// </summary>
    internal class BigQueryTestConfiguration : TestConfiguration
    {
        public BigQueryTestConfiguration()
        {
            AllowLargeResults = false;
            IncludeTableConstraints = true;
        }

        [JsonPropertyName("projectId")]
        public string ProjectId { get; set; } = string.Empty;

        [JsonPropertyName("clientId")]
        public string ClientId { get; set; } = string.Empty;

        [JsonPropertyName("clientSecret")]
        public string ClientSecret { get; set; } = string.Empty;

        [JsonPropertyName("refreshToken")]
        public string RefreshToken { get; set; } = string.Empty;

        [JsonPropertyName("scopes")]
        public string Scopes { get; set; } = string.Empty;

        [JsonPropertyName("jsonCredential")]
        public string JsonCredential { get; set; } = string.Empty;

        [JsonPropertyName("allowLargeResults")]
        public bool AllowLargeResults { get; set; }

        [JsonPropertyName("largeResultsDestinationTable")]
        public string LargeResultsDestinationTable { get; set; } = string.Empty;

        [JsonPropertyName("includeTableConstraints")]
        public bool IncludeTableConstraints { get; set; }
    }
}
