using System.Text.Json.Serialization;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class OperationDetail
    {
        [JsonPropertyName("n_operation_status_calls")]
        public int NOperationStatusCalls { get; set; }

        [JsonPropertyName("operation_status_latency_millis")]
        public long OperationStatusLatencyMillis { get; set; }

        [JsonPropertyName("operation_type")]
        public OperationType OperationType { get; set; }

        [JsonPropertyName("is_internal_call")]
        public bool IsInternalCall { get; set; }
    }
}