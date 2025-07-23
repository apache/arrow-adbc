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