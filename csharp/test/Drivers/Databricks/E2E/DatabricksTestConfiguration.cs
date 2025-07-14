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
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    public class DatabricksTestConfiguration : SparkTestConfiguration
    {
        [JsonPropertyName("grant_type"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string OAuthGrantType { get; set; } = string.Empty;

        [JsonPropertyName("client_id"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string OAuthClientId { get; set; } = string.Empty;

        [JsonPropertyName("client_secret"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string OAuthClientSecret { get; set; } = string.Empty;

        [JsonPropertyName("scope"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string OAuthScope { get; set; } = string.Empty;

        [JsonPropertyName("enableMultipleCatalogSupport")]
        public string EnableMultipleCatalogSupport { get; set; } = string.Empty;

        [JsonPropertyName("tracePropagationEnabled"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string TracePropagationEnabled { get; set; } = string.Empty;

        [JsonPropertyName("traceParentHeaderName"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string TraceParentHeaderName { get; set; } = string.Empty;

        [JsonPropertyName("traceStateEnabled"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string TraceStateEnabled { get; set; } = string.Empty;

        [JsonPropertyName("isCITesting"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public bool IsCITesting { get; set; } = false;
    }
}
