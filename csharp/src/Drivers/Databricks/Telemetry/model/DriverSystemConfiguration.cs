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
