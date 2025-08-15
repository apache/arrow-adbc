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

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public static class HiveServer2Parameters
    {
        public const string HostName = "adbc.hive.host";
        public const string Port = "adbc.hive.port";
        public const string Path = "adbc.hive.path";
        public const string AuthType = "adbc.hive.auth_type";
        public const string TransportType = "adbc.hive.transport_type";
        public const string DataTypeConv = "adbc.hive.data_type_conv";
        public const string ConnectTimeoutMilliseconds = "adbc.hive.connect_timeout_ms";
        public const string FetchResultsTimeoutSeconds = "adbc.hive.fetch_results_timeout_s";
    }

    public static class HiveServer2AuthTypeConstants
    {
        public const string None = "none";
        public const string UsernameOnly = "username_only";
        public const string Basic = "basic";
    }

    public static class HiveServer2TransportTypeConstants
    {
        public const string Http = "http";
        public const string Standard = "standard";
    }

    public static class DataTypeConversionOptions
    {
        public const string None = "none";
        public const string Scalar = "scalar";
    }

    public static class HttpTlsOptions
    {
        public const string IsTlsEnabled = "adbc.http_options.tls.enabled";
        public const string AllowSelfSigned = "adbc.http_options.tls.allow_self_signed";
        public const string AllowHostnameMismatch = "adbc.http_options.tls.allow_hostname_mismatch";
        public const string TrustedCertificatePath = "adbc.http_options.tls.trusted_certificate_path";
        public const string DisableServerCertificateValidation = "adbc.http_options.tls.disable_server_certificate_validation";
    }

    public static class StandardTlsOptions
    {
        public const string IsTlsEnabled = "adbc.standard_options.tls.enabled";
        public const string AllowSelfSigned = "adbc.standard_options.tls.allow_self_signed";
        public const string AllowHostnameMismatch = "adbc.standard_options.tls.allow_hostname_mismatch";
        public const string TrustedCertificatePath = "adbc.standard_options.tls.trusted_certificate_path";
        public const string DisableServerCertificateValidation = "adbc.standard_options.tls.disable_server_certificate_validation";
    }

    public static class HttpProxyOptions
    {
        /// <summary>
        /// Whether to use a proxy for HTTP connections.
        /// Default value is false (disabled) if not specified.
        /// </summary>
        public const string UseProxy = "adbc.proxy_options.use_proxy";

        /// <summary>
        /// Hostname or IP address of the proxy server.
        /// Required when UseProxy.
        /// </summary>
        public const string ProxyHost = "adbc.proxy_options.proxy_host";

        /// <summary>
        /// Port number of the proxy server.
        /// Required when UseProxy.
        /// </summary>
        public const string ProxyPort = "adbc.proxy_options.proxy_port";

        /// <summary>
        /// Comma-separated list of hosts or domains that should bypass the proxy.
        /// For example: "localhost,127.0.0.1,.internal.domain.com"
        /// Allows for wildcard pattern matching, i.e. "*.internal.domain.com"
        /// </summary>
        public const string ProxyIgnoreList = "adbc.proxy_options.proxy_ignore_list";

        /// <summary>
        /// Whether to enable proxy authentication.
        /// Default value is false (disabled) if not specified.
        /// </summary>
        public const string ProxyAuth = "adbc.proxy_options.proxy_auth";

        /// <summary>
        /// Username for proxy authentication.
        /// Required when ProxyAuth.
        /// </summary>
        public const string ProxyUID = "adbc.proxy_options.proxy_uid";

        /// <summary>
        /// Password for proxy authentication.
        /// Required when ProxyAuth.
        /// </summary>
        public const string ProxyPWD = "adbc.proxy_options.proxy_pwd";
    }
}
