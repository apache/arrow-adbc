/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System.Text.Json.Serialization;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    internal class TelemetryEvent
    {
        [JsonPropertyName("session_id")]
        public string? SessionId { get; set; }

        [JsonPropertyName("sql_statement_id")]
        public string? SqlStatementId { get; set; }

        [JsonPropertyName("system_configuration")]
        public DriverSystemConfiguration? SystemConfiguration { get; set; }
        
        [JsonPropertyName("driver_connection_params")]
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
        public long? LatencyMs { get; set; }
    }
}
