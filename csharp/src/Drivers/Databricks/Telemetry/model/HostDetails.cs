using System.Text.Json.Serialization;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class HostDetails
    {
        [JsonPropertyName("host_url")]
        public string? HostUrl { get; set; }

        [JsonPropertyName("port")]
        public int Port { get; set; }

        [JsonPropertyName("proxy_auth_type")]
        public DriverProxy proxyType { get; set; }
    }
}