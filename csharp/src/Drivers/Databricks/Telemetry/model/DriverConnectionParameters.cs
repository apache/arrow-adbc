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
using Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Enums;

namespace Apache.Arrow.Adbc.Drivers.Databricks.Telemetry.Model
{
    public class DriverConnectionParameters
    {
        [JsonPropertyName("http_path")]
        public string? HTTPPath { get; set; }

        [JsonPropertyName("mode")]
        public string? DriverMode { get; set; }

        [JsonPropertyName("host_info")]
        public HostDetails? HostInfo { get; set; }

        [JsonPropertyName("use_proxy")]
        public bool UseProxy { get; set; }

        [JsonPropertyName("auth_mech")]
        public AuthMech AuthMech { get; set; }

        [JsonPropertyName("auth_flow")]
        public AuthFlow AuthFlow { get; set; }

        [JsonPropertyName("auth_scope")]
        public string? AuthScope { get; set; }

        [JsonPropertyName("use_system_proxy")]
        public bool UseSystemProxy { get; set; }

        [JsonPropertyName("non_proxy_hosts")]
        public string[]? NonProxyHosts { get; set; }

        [JsonPropertyName("use_cf_proxy")]
        public bool UseCfProxy { get; set; }

        [JsonPropertyName("proxy_host_info")]
        public HostDetails? ProxyHostInfo { get; set; }

        [JsonPropertyName("cf_proxy_host_info")]
        public HostDetails? CfProxyHostInfo { get; set; }

        [JsonPropertyName("discovery_mode_enabled")]
        public bool DiscoveryModeEnabled { get; set; }

        [JsonPropertyName("discovery_url")]
        public string? DiscoveryUrl { get; set; }

        [JsonPropertyName("use_empty_metadata")]
        public bool UseEmptyMetadata { get; set; }

        [JsonPropertyName("support_many_parameters")]
        public bool SupportManyParameters { get; set; }

        [JsonPropertyName("ssl_trust_store_type")]
        public string? SslTrustStoreType { get; set; }

        [JsonPropertyName("check_certificate_revocation")]
        public bool CheckCertificateRevocation { get; set; }

        [JsonPropertyName("accept_undetermined_certificate_revocation")]
        public bool AcceptUndeterminedCertificateRevocation { get; set; }

        [JsonPropertyName("google_service_account")]
        public string? GoogleServiceAccount { get; set; }

        [JsonPropertyName("google_credential_file_path")]
        public string? GoogleCredentialFilePath { get; set; }

        [JsonPropertyName("http_connection_pool_size")]
        public long HttpConnectionPoolSize { get; set; }

        [JsonPropertyName("enable_sea_hybrid_results")]
        public bool EnableSeaHybridResults { get; set; }

        [JsonPropertyName("enable_complex_datatype_support")]
        public bool EnableComplexDatatypeSupport { get; set; }

        [JsonPropertyName("allow_self_signed_support")]
        public bool AllowSelfSignedSupport { get; set; }

        [JsonPropertyName("use_system_trust_store")]
        public bool UseSystemTrustStore { get; set; }

        [JsonPropertyName("rows_fetched_per_block")]
        public long RowsFetchedPerBlock { get; set; }

        [JsonPropertyName("azure_workspace_resource_id")]
        public bool AzureWorkspaceResourceId { get; set; }

        [JsonPropertyName("azure_tenant_id")]
        public string? AzureTenantId { get; set; }

        [JsonPropertyName("string_column_length")]
        public int StringColumnLength { get; set; }

        [JsonPropertyName("enable_token_cache")]
        public bool EnableTokenCache { get; set; }

        [JsonPropertyName("token_endpoint")]
        public string? TokenEndpoint { get; set; }

        [JsonPropertyName("auth_endpoint")]
        public string? AuthEndpoint { get; set; }

        [JsonPropertyName("enable_arrow")]
        public bool EnableArrow { get; set; }

        [JsonPropertyName("enable_direct_results")]
        public bool EnableDirectResults { get; set; }

        [JsonPropertyName("enable_jwt_assertion")]
        public bool EnableJwtAssertion { get; set; }

        [JsonPropertyName("jwt_key_file")]
        public bool JwtKeyFile { get; set; }

        [JsonPropertyName("jwt_algorithm")]
        public string? JwtAlgorithm { get; set; }

        [JsonPropertyName("socket_timeout")]
        public long SocketTimeout { get; set; }

        [JsonPropertyName("allowed_volume_ingestion_paths")]
        public long AllowedVolumeIngestionPaths { get; set; }
        
        [JsonPropertyName("async_poll_interval_millis")]
        public long AsyncPollIntervalMillis { get; set; }
    }
}