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