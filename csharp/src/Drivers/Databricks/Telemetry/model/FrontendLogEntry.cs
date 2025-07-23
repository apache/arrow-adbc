using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class FrontendLogEntry
    {
        [JsonPropertyName("sql_driver_log")]
        public TelemetryEvent? SqlDriverLog { get; set; }
    }
}