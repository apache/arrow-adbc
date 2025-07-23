using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class ResultLatency
    {
        [JsonPropertyName("result_set_ready_latency_millis")]
        public long ResultSetReadyLatencyMillis { get; set; }

        [JsonPropertyName("result_set_consumption_latency_millis")]
        public long ResultSetConsumptionLatencyMillis { get; set; }
    }
}