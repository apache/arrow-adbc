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

using System;
using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Base test configuration values.
    /// </summary>
    public abstract class TestConfiguration : ICloneable
    {
        /// <summary>
        /// The query to run.
        /// </summary>
        [JsonPropertyName("query")]
        public string Query { get; set; } = string.Empty;

        /// <summary>
        /// The number of expected results from the query.
        /// </summary>
        [JsonPropertyName("expectedResults")]
        public long ExpectedResultsCount { get; set; }

        /// <summary>
        /// The <see cref="TestMetadata"/> details.
        /// </summary>
        [JsonPropertyName("metadata")]
        public TestMetadata Metadata { get; set; } = new TestMetadata();

        public virtual object Clone() => MemberwiseClone();
    }

    /// <summary>
    /// The values for the ADBC metadata calls.
    /// </summary>
    public class TestMetadata
    {
        /// <summary>
        /// The name of the catalog/database.
        /// </summary>
        [JsonPropertyName("catalog")]
        public string Catalog { get; set; } = string.Empty;

        /// <summary>
        /// The name of the schema.
        /// </summary>
        [JsonPropertyName("schema")]
        public string Schema { get; set; } = string.Empty;

        /// <summary>
        /// The name of the table.
        /// </summary>
        [JsonPropertyName("table")]
        public string Table { get; set; } = string.Empty;

        /// <summary>
        /// The number of columns expected from the table.
        /// </summary>
        [JsonPropertyName("expectedColumnCount")]
        public int ExpectedColumnCount { get; set; }
    }
}
