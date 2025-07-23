using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class FrontendLogContext
    {
        [JsonPropertyName("client_context")]
        public ClientContext? ClientContext { get; set; }
    }
}