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
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class SqlExecutionEvent
    {
        [JsonPropertyName("statement_type")]
        public StatementType StatementType { get; set; }

        [JsonPropertyName("is_compressed")]
        public bool IsCompressed { get; set; }

        [JsonPropertyName("execution_result")]
        public ExecutionResultFormat ExecutionResult { get; set; }

        [JsonPropertyName("chunk_id")]
        public long ChunkId { get; set; }
        
        [JsonPropertyName("retry_count")]
        public long RetryCount { get; set; }

        [JsonPropertyName("chunk_details")]
        public ChunkDetails? ChunkDetails { get; set; }

        [JsonPropertyName("result_latency")]
        public ResultLatency? ResultLatency { get; set; }

        [JsonPropertyName("operation_detail")]
        public OperationDetail? OperationDetail { get; set; }
    }
}