using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class TelemetryFrontendLog
    {
        [JsonPropertyName("workspace_id")]
        public long WorkspaceId { get; set; }

        [JsonPropertyName("frontend_log_event_id")]
        public string? FrontendLogEventId { get; set; }

        [JsonPropertyName("entry")]
        public FrontendLogEntry? Entry { get; set; }

        [JsonPropertyName("context")]
        public FrontendLogContext? Context { get; set; }
    }
}