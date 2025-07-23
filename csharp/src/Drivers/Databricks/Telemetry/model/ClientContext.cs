using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class ClientContext
    {
        [JsonPropertyName("timestamp_millis")]
        public long TimestampMillis { get; set; }

        [JsonPropertyName("user_agent")]
        public string? UserAgent { get; set; }
    }
}