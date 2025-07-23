using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class DriverSystemConfiguration
    {
        [JsonPropertyName("driver_version")]
        public string? DriverVersion { get; set; }

        [JsonPropertyName("os_name")]
        public string? OsName { get; set; }

        [JsonPropertyName("os_version")]
        public string? OsVersion { get; set; }

        [JsonPropertyName("os_arch")]
        public string? OsArch { get; set; }

        [JsonPropertyName("runtime_name")]
        public string? RuntimeName { get; set; }

        [JsonPropertyName("runtime_version")]
        public string? RuntimeVersion { get; set; }

        [JsonPropertyName("runtime_vendor")]
        public string? RuntimeVendor { get; set; }
        
        [JsonPropertyName("client_app_name")]
        public string? ClientAppName { get; set; }

        [JsonPropertyName("locale_name")]
        public string? LocaleName { get; set; }

        [JsonPropertyName("driver_name")]
        public string? DriverName { get; set; }

        [JsonPropertyName("char_set_encoding")]
        public string? CharSetEncoding { get; set; }

        [JsonPropertyName("process_name")]
        public string? ProcessName { get; set; }
    }
}