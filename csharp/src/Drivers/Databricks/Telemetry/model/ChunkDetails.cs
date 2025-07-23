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

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class ChunkDetails
    {
        [JsonPropertyName("initial_chunk_latency_millis")]
        public long InitialChunkLatencyMillis { get; set; }

        [JsonPropertyName("slowest_chunk_latency_millis")]
        public long SlowestChunkLatencyMillis { get; set; }

        [JsonPropertyName("total_chunks_present")]
        public long TotalChunksPresent { get; set; }

        [JsonPropertyName("total_chunks_iterated")]
        public long TotalChunksIterated { get; set; }

        [JsonPropertyName("sum_chunks_download_time_millis")]
        public long SumChunksDownloadTimeMillis { get; set; }

        public ChunkDetails(long totalChunks)
        {
            this.TotalChunksPresent = totalChunks;
            this.TotalChunksIterated = 0;
            this.SumChunksDownloadTimeMillis = 0;
        }
    }
}