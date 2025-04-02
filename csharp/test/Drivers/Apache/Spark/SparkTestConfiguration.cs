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

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class SparkTestConfiguration : ApacheTestConfiguration
    {

        [JsonPropertyName("token"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Token { get; set; } = string.Empty;

        [JsonPropertyName("access_token"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string AccessToken { get; set; } = string.Empty;

        [JsonPropertyName("cloud_fetch_metadata"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public CloudFetchMetadata? CloudFetchMetadata { get; set; }
    }

    public class CloudFetchMetadata
    {
        [JsonPropertyName("catalog"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Catalog { get; set; } = string.Empty;

        [JsonPropertyName("schema"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Schema { get; set; } = string.Empty;

        [JsonPropertyName("table"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Table { get; set; } = string.Empty;

        [JsonPropertyName("expectedResults"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public int? ExpectedResults { get; set; }
    }
}
