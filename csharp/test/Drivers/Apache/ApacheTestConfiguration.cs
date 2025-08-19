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

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache
{
    public class ApacheTestConfiguration : TestConfiguration
    {
        [JsonPropertyName("hostName")]
        public string HostName { get; set; } = string.Empty;

        [JsonPropertyName("port")]
        public string Port { get; set; } = string.Empty;

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

        [JsonPropertyName("polltime_ms"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string PollTimeMilliseconds { get; set; } = string.Empty;

        [JsonPropertyName("connect_timeout_ms"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string ConnectTimeoutMilliseconds { get; set; } = string.Empty;

        [JsonPropertyName("query_timeout_s"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string QueryTimeoutSeconds { get; set; } = string.Empty;

        [JsonPropertyName("type"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Type { get; set; } = string.Empty;

        [JsonPropertyName("data_type_conv"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string DataTypeConversion { get; set; } = string.Empty;

        [JsonPropertyName("http_options"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public HttpTestConfiguration? HttpOptions { get; set; }

        [JsonPropertyName("catalog"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string Catalog { get; set; } = string.Empty;

        [JsonPropertyName("db_schema"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string DbSchema { get; set; } = string.Empty;
    }

    public class HttpTestConfiguration
    {
        [JsonPropertyName("tls"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public TlsTestConfiguration? Tls { get; set; }

        [JsonPropertyName("proxy"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public ProxyTestConfiguration? Proxy { get; set; }
    }

    public class ProxyTestConfiguration
    {
        [JsonPropertyName("use_proxy"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string? UseProxy { get; set; }

        [JsonPropertyName("proxy_host"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string? ProxyHost { get; set; }

        [JsonPropertyName("proxy_port"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public int? ProxyPort { get; set; }

        [JsonPropertyName("proxy_auth"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string? ProxyAuth { get; set; }

        [JsonPropertyName("proxy_uid"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string? ProxyUid { get; set; }

        [JsonPropertyName("proxy_pwd"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string? ProxyPwd { get; set; }

        [JsonPropertyName("proxy_ignore_list"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string? ProxyIgnoreList { get; set; }
    }

    public class TlsTestConfiguration
    {
        [JsonPropertyName("enabled"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public bool? Enabled { get; set; }

        [JsonPropertyName("disable_server_certificate_validation"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public bool? DisableServerCertificateValidation { get; set; }

        [JsonPropertyName("allow_self_signed"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public bool? AllowSelfSigned { get; set; }

        [JsonPropertyName("allow_hostname_mismatch"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public bool? AllowHostnameMismatch { get; set; }

        [JsonPropertyName("trusted_certificate_path"), JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingDefault)]
        public string? TrustedCertificatePath { get; set; }
    }
}
