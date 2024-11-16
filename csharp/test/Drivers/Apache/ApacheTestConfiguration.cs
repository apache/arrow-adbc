﻿/*
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

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache
{
    public class ApacheTestConfiguration : TestConfiguration
    {
        [JsonPropertyName("hostName")]
        public string HostName { get; set; } = string.Empty;

        [JsonPropertyName("port")]
        public string Port { get; set; } = string.Empty;

        [JsonPropertyName("token"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Token { get; set; } = string.Empty;

        [JsonPropertyName("path"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Path { get; set; } = string.Empty;

        [JsonPropertyName("username"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Username { get; set; } = string.Empty;

        [JsonPropertyName("password"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Password { get; set; } = string.Empty;

        [JsonPropertyName("auth_type"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string AuthType { get; set; } = string.Empty;

        [JsonPropertyName("uri"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Uri { get; set; } = string.Empty;

        [JsonPropertyName("batch_size"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string BatchSize { get; set; } = string.Empty;

        [JsonPropertyName("polltime_milliseconds"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string PollTimeMilliseconds { get; set; } = string.Empty;

        [JsonPropertyName("http_request_timeout_ms"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string HttpRequestTimeoutMilliseconds { get; set; } = string.Empty;

        [JsonPropertyName("trace"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Trace { get; set; } = string.Empty;

        [JsonPropertyName("trace_location"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string TraceLocation { get; set; } = string.Empty;

        [JsonPropertyName("trace_max_size_kb"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string TraceFileMaxSizeKb { get; set; } = string.Empty;

        [JsonPropertyName("trace_max_files"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string TraceFileMaxFiles { get; set; } = string.Empty;

    }
}
