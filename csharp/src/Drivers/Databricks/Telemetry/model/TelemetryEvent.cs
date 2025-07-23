using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class TelemetryEvent
    {
        [JsonPropertyName("session_id")]
        public string? SessionId { get; set; }

        [JsonPropertyName("sql_statement_id")]
        public string? SqlStatementId { get; set; }

        [JsonPropertyName("system_configuration")]
        public DriverSystemConfiguration? SystemConfiguration { get; set; }
        
        [JsonPropertyName("driver_connection_parameters")]
        public DriverConnectionParameters? DriverConnectionParameters { get; set; }

        [JsonPropertyName("auth_type")]
        public string? AuthType { get; set; }

        [JsonPropertyName("vol_operation")]
        public DriverVolumeOperation? VolumeOperation { get; set; }

        [JsonPropertyName("sql_operation")]
        public SqlExecutionEvent? SqlExecutionEvent { get; set; }

        [JsonPropertyName("error_info")]
        public DriverErrorInfo? ErrorInfo { get; set; }

        [JsonPropertyName("operation_latency_ms")]
        public long LatencyMs { get; set; }
    }
}