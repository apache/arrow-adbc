using System.Text.Json.Serialization;
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class DriverVolumeOperation
    {
        [JsonPropertyName("operation_type")]
        public DriverVolumeOperationType OperationType { get; set; }

        [JsonPropertyName("volume_path")]
        public string? VolumePath { get; set; }
    }
}