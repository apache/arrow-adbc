using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class DriverErrorInfo
    {
        [JsonPropertyName("error_name")]
        public string? ErrorName { get; set; }

        [JsonPropertyName("stack_trace")]
        public string? StackTrace { get; set; }
    }
}